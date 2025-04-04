package com.dbapp;
import static org.junit.Assert.assertTrue;
import java.util.*;
import org.junit.Test;
public class DBAppTests03
{

	@Test(timeout = 1000000)
	public void test162TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"mo5v","faz0b"};
		DBApp.createTable("k09", cols0);
		for(int i=0;i<34;i++)
		{
			String [] record_k09 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_k09[j] = genRandString();
			}
			DBApp.insert("k09", record_k09);
		}
		int pagesCount_k09 = (int)Math.ceil(34.0/DBApp.dataPageSize);
		ArrayList<String> res_k09 = new ArrayList<>();
		for(int i=0;i<pagesCount_k09;i++)
		{
			res_k09.add(i+".db");
		}
		Collections.sort(res_k09);
		String pagesTraceString_k09 = "k09{ " + String.join(" ",res_k09);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_k09+")", fileManagerTrace0.contains(pagesTraceString_k09));
		//--------------------------------------------------------------------------
		String[] cols1 = {"d4h","y7y","rk2","w0"};
		DBApp.createTable("g9qr", cols1);
		for(int i=0;i<37;i++)
		{
			String [] record_g9qr = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_g9qr[j] = genRandString();
			}
			DBApp.insert("g9qr", record_g9qr);
		}
		int pagesCount_g9qr = (int)Math.ceil(37.0/DBApp.dataPageSize);
		ArrayList<String> res_g9qr = new ArrayList<>();
		for(int i=0;i<pagesCount_g9qr;i++)
		{
			res_g9qr.add(i+".db");
		}
		Collections.sort(res_g9qr);
		String pagesTraceString_g9qr = "g9qr{ " + String.join(" ",res_g9qr);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_g9qr+")", fileManagerTrace1.contains(pagesTraceString_g9qr));
		//--------------------------------------------------------------------------
		String[] cols2 = {"rz40","y9p97","es","ji","rd1","cs56j"};
		DBApp.createTable("b943", cols2);
		for(int i=0;i<15;i++)
		{
			String [] record_b943 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_b943[j] = genRandString();
			}
			DBApp.insert("b943", record_b943);
		}
		int pagesCount_b943 = (int)Math.ceil(15.0/DBApp.dataPageSize);
		ArrayList<String> res_b943 = new ArrayList<>();
		for(int i=0;i<pagesCount_b943;i++)
		{
			res_b943.add(i+".db");
		}
		Collections.sort(res_b943);
		String pagesTraceString_b943 = "b943{ " + String.join(" ",res_b943);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_b943+")", fileManagerTrace2.contains(pagesTraceString_b943));
		//--------------------------------------------------------------------------
		String[] cols3 = {"ckt","ks","ren","le8p2","sm9","f6ml","ks6"};
		DBApp.createTable("r18", cols3);
		for(int i=0;i<16;i++)
		{
			String [] record_r18 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_r18[j] = genRandString();
			}
			DBApp.insert("r18", record_r18);
		}
		int pagesCount_r18 = (int)Math.ceil(16.0/DBApp.dataPageSize);
		ArrayList<String> res_r18 = new ArrayList<>();
		for(int i=0;i<pagesCount_r18;i++)
		{
			res_r18.add(i+".db");
		}
		Collections.sort(res_r18);
		String pagesTraceString_r18 = "r18{ " + String.join(" ",res_r18);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_r18+")", fileManagerTrace3.contains(pagesTraceString_r18));
		//--------------------------------------------------------------------------
		String[] cols4 = {"hx9","wz4","d0","bgwqk","g6c"};
		DBApp.createTable("moo", cols4);
		for(int i=0;i<52;i++)
		{
			String [] record_moo = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_moo[j] = genRandString();
			}
			DBApp.insert("moo", record_moo);
		}
		int pagesCount_moo = (int)Math.ceil(52.0/DBApp.dataPageSize);
		ArrayList<String> res_moo = new ArrayList<>();
		for(int i=0;i<pagesCount_moo;i++)
		{
			res_moo.add(i+".db");
		}
		Collections.sort(res_moo);
		String pagesTraceString_moo = "moo{ " + String.join(" ",res_moo);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_moo+")", fileManagerTrace4.contains(pagesTraceString_moo));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test163TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"o7","oe"};
		DBApp.createTable("f3op", cols0);
		for(int i=0;i<18;i++)
		{
			String [] record_f3op = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_f3op[j] = genRandString();
			}
			DBApp.insert("f3op", record_f3op);
		}
		int pagesCount_f3op = (int)Math.ceil(18.0/DBApp.dataPageSize);
		ArrayList<String> res_f3op = new ArrayList<>();
		for(int i=0;i<pagesCount_f3op;i++)
		{
			res_f3op.add(i+".db");
		}
		Collections.sort(res_f3op);
		String pagesTraceString_f3op = "f3op{ " + String.join(" ",res_f3op);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_f3op+")", fileManagerTrace0.contains(pagesTraceString_f3op));
		//--------------------------------------------------------------------------
		String[] cols1 = {"z0ho","oe","geh1","q3","ba165","mf","bjr56"};
		DBApp.createTable("e8", cols1);
		for(int i=0;i<61;i++)
		{
			String [] record_e8 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_e8[j] = genRandString();
			}
			DBApp.insert("e8", record_e8);
		}
		int pagesCount_e8 = (int)Math.ceil(61.0/DBApp.dataPageSize);
		ArrayList<String> res_e8 = new ArrayList<>();
		for(int i=0;i<pagesCount_e8;i++)
		{
			res_e8.add(i+".db");
		}
		Collections.sort(res_e8);
		String pagesTraceString_e8 = "e8{ " + String.join(" ",res_e8);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_e8+")", fileManagerTrace1.contains(pagesTraceString_e8));
		//--------------------------------------------------------------------------
		String[] cols2 = {"h7md5","p1dn","k3nv","l041","ae","gy"};
		DBApp.createTable("v7x", cols2);
		for(int i=0;i<68;i++)
		{
			String [] record_v7x = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_v7x[j] = genRandString();
			}
			DBApp.insert("v7x", record_v7x);
		}
		int pagesCount_v7x = (int)Math.ceil(68.0/DBApp.dataPageSize);
		ArrayList<String> res_v7x = new ArrayList<>();
		for(int i=0;i<pagesCount_v7x;i++)
		{
			res_v7x.add(i+".db");
		}
		Collections.sort(res_v7x);
		String pagesTraceString_v7x = "v7x{ " + String.join(" ",res_v7x);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_v7x+")", fileManagerTrace2.contains(pagesTraceString_v7x));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test164TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"gx","ov","d0rf1","outt","a5a7","i9","u2w","ag6","yde"};
		DBApp.createTable("nb", cols0);
		for(int i=0;i<23;i++)
		{
			String [] record_nb = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_nb[j] = genRandString();
			}
			DBApp.insert("nb", record_nb);
		}
		int pagesCount_nb = (int)Math.ceil(23.0/DBApp.dataPageSize);
		ArrayList<String> res_nb = new ArrayList<>();
		for(int i=0;i<pagesCount_nb;i++)
		{
			res_nb.add(i+".db");
		}
		Collections.sort(res_nb);
		String pagesTraceString_nb = "nb{ " + String.join(" ",res_nb);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_nb+")", fileManagerTrace0.contains(pagesTraceString_nb));
		//--------------------------------------------------------------------------
		String[] cols1 = {"qr2b","u6xn"};
		DBApp.createTable("ez2", cols1);
		for(int i=0;i<22;i++)
		{
			String [] record_ez2 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_ez2[j] = genRandString();
			}
			DBApp.insert("ez2", record_ez2);
		}
		int pagesCount_ez2 = (int)Math.ceil(22.0/DBApp.dataPageSize);
		ArrayList<String> res_ez2 = new ArrayList<>();
		for(int i=0;i<pagesCount_ez2;i++)
		{
			res_ez2.add(i+".db");
		}
		Collections.sort(res_ez2);
		String pagesTraceString_ez2 = "ez2{ " + String.join(" ",res_ez2);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ez2+")", fileManagerTrace1.contains(pagesTraceString_ez2));
		//--------------------------------------------------------------------------
		String[] cols2 = {"f9","oee","svs5g","t73"};
		DBApp.createTable("z83", cols2);
		for(int i=0;i<23;i++)
		{
			String [] record_z83 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_z83[j] = genRandString();
			}
			DBApp.insert("z83", record_z83);
		}
		int pagesCount_z83 = (int)Math.ceil(23.0/DBApp.dataPageSize);
		ArrayList<String> res_z83 = new ArrayList<>();
		for(int i=0;i<pagesCount_z83;i++)
		{
			res_z83.add(i+".db");
		}
		Collections.sort(res_z83);
		String pagesTraceString_z83 = "z83{ " + String.join(" ",res_z83);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_z83+")", fileManagerTrace2.contains(pagesTraceString_z83));
		//--------------------------------------------------------------------------
		String[] cols3 = {"vax","wt","i1az","i7c5","g3d"};
		DBApp.createTable("ldj6", cols3);
		for(int i=0;i<31;i++)
		{
			String [] record_ldj6 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_ldj6[j] = genRandString();
			}
			DBApp.insert("ldj6", record_ldj6);
		}
		int pagesCount_ldj6 = (int)Math.ceil(31.0/DBApp.dataPageSize);
		ArrayList<String> res_ldj6 = new ArrayList<>();
		for(int i=0;i<pagesCount_ldj6;i++)
		{
			res_ldj6.add(i+".db");
		}
		Collections.sort(res_ldj6);
		String pagesTraceString_ldj6 = "ldj6{ " + String.join(" ",res_ldj6);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ldj6+")", fileManagerTrace3.contains(pagesTraceString_ldj6));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test165TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"bstop","b39v3","qez","w7010","pj","t164","g3b4r","s21c1","qv","ncy"};
		DBApp.createTable("gv7n", cols0);
		for(int i=0;i<16;i++)
		{
			String [] record_gv7n = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_gv7n[j] = genRandString();
			}
			DBApp.insert("gv7n", record_gv7n);
		}
		int pagesCount_gv7n = (int)Math.ceil(16.0/DBApp.dataPageSize);
		ArrayList<String> res_gv7n = new ArrayList<>();
		for(int i=0;i<pagesCount_gv7n;i++)
		{
			res_gv7n.add(i+".db");
		}
		Collections.sort(res_gv7n);
		String pagesTraceString_gv7n = "gv7n{ " + String.join(" ",res_gv7n);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_gv7n+")", fileManagerTrace0.contains(pagesTraceString_gv7n));
		//--------------------------------------------------------------------------
		String[] cols1 = {"gl","q8c","sun","e95","u62","q3","fb0","ku9","a9y","s1"};
		DBApp.createTable("y91", cols1);
		for(int i=0;i<56;i++)
		{
			String [] record_y91 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_y91[j] = genRandString();
			}
			DBApp.insert("y91", record_y91);
		}
		int pagesCount_y91 = (int)Math.ceil(56.0/DBApp.dataPageSize);
		ArrayList<String> res_y91 = new ArrayList<>();
		for(int i=0;i<pagesCount_y91;i++)
		{
			res_y91.add(i+".db");
		}
		Collections.sort(res_y91);
		String pagesTraceString_y91 = "y91{ " + String.join(" ",res_y91);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_y91+")", fileManagerTrace1.contains(pagesTraceString_y91));
		//--------------------------------------------------------------------------
		String[] cols2 = {"lu6","zm","v67l2","t689n","v3","c0r"};
		DBApp.createTable("uhh0q", cols2);
		for(int i=0;i<61;i++)
		{
			String [] record_uhh0q = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_uhh0q[j] = genRandString();
			}
			DBApp.insert("uhh0q", record_uhh0q);
		}
		int pagesCount_uhh0q = (int)Math.ceil(61.0/DBApp.dataPageSize);
		ArrayList<String> res_uhh0q = new ArrayList<>();
		for(int i=0;i<pagesCount_uhh0q;i++)
		{
			res_uhh0q.add(i+".db");
		}
		Collections.sort(res_uhh0q);
		String pagesTraceString_uhh0q = "uhh0q{ " + String.join(" ",res_uhh0q);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_uhh0q+")", fileManagerTrace2.contains(pagesTraceString_uhh0q));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test166TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"bx","tos58","y0","at","xr","fw64","yv2"};
		DBApp.createTable("yzs", cols0);
		for(int i=0;i<60;i++)
		{
			String [] record_yzs = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_yzs[j] = genRandString();
			}
			DBApp.insert("yzs", record_yzs);
		}
		int pagesCount_yzs = (int)Math.ceil(60.0/DBApp.dataPageSize);
		ArrayList<String> res_yzs = new ArrayList<>();
		for(int i=0;i<pagesCount_yzs;i++)
		{
			res_yzs.add(i+".db");
		}
		Collections.sort(res_yzs);
		String pagesTraceString_yzs = "yzs{ " + String.join(" ",res_yzs);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_yzs+")", fileManagerTrace0.contains(pagesTraceString_yzs));
		//--------------------------------------------------------------------------
		String[] cols1 = {"n0q","j2y","p5","tqc64","t7812","lz","x9pl","h0r55","h4u","ffc"};
		DBApp.createTable("uq1qq", cols1);
		for(int i=0;i<23;i++)
		{
			String [] record_uq1qq = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_uq1qq[j] = genRandString();
			}
			DBApp.insert("uq1qq", record_uq1qq);
		}
		int pagesCount_uq1qq = (int)Math.ceil(23.0/DBApp.dataPageSize);
		ArrayList<String> res_uq1qq = new ArrayList<>();
		for(int i=0;i<pagesCount_uq1qq;i++)
		{
			res_uq1qq.add(i+".db");
		}
		Collections.sort(res_uq1qq);
		String pagesTraceString_uq1qq = "uq1qq{ " + String.join(" ",res_uq1qq);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_uq1qq+")", fileManagerTrace1.contains(pagesTraceString_uq1qq));
		//--------------------------------------------------------------------------
		String[] cols2 = {"vk","n0q2s","yc6"};
		DBApp.createTable("sl5", cols2);
		for(int i=0;i<21;i++)
		{
			String [] record_sl5 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_sl5[j] = genRandString();
			}
			DBApp.insert("sl5", record_sl5);
		}
		int pagesCount_sl5 = (int)Math.ceil(21.0/DBApp.dataPageSize);
		ArrayList<String> res_sl5 = new ArrayList<>();
		for(int i=0;i<pagesCount_sl5;i++)
		{
			res_sl5.add(i+".db");
		}
		Collections.sort(res_sl5);
		String pagesTraceString_sl5 = "sl5{ " + String.join(" ",res_sl5);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_sl5+")", fileManagerTrace2.contains(pagesTraceString_sl5));
		//--------------------------------------------------------------------------
		String[] cols3 = {"c6073","q08","b9e","yk6","p4p35","qsz","u2p"};
		DBApp.createTable("t7a", cols3);
		for(int i=0;i<40;i++)
		{
			String [] record_t7a = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_t7a[j] = genRandString();
			}
			DBApp.insert("t7a", record_t7a);
		}
		int pagesCount_t7a = (int)Math.ceil(40.0/DBApp.dataPageSize);
		ArrayList<String> res_t7a = new ArrayList<>();
		for(int i=0;i<pagesCount_t7a;i++)
		{
			res_t7a.add(i+".db");
		}
		Collections.sort(res_t7a);
		String pagesTraceString_t7a = "t7a{ " + String.join(" ",res_t7a);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_t7a+")", fileManagerTrace3.contains(pagesTraceString_t7a));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test167TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"m208","oeij2","s98","w158"};
		DBApp.createTable("w877", cols0);
		for(int i=0;i<91;i++)
		{
			String [] record_w877 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_w877[j] = genRandString();
			}
			DBApp.insert("w877", record_w877);
		}
		int pagesCount_w877 = (int)Math.ceil(91.0/DBApp.dataPageSize);
		ArrayList<String> res_w877 = new ArrayList<>();
		for(int i=0;i<pagesCount_w877;i++)
		{
			res_w877.add(i+".db");
		}
		Collections.sort(res_w877);
		String pagesTraceString_w877 = "w877{ " + String.join(" ",res_w877);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_w877+")", fileManagerTrace0.contains(pagesTraceString_w877));
		//--------------------------------------------------------------------------
		String[] cols1 = {"wp","p0x"};
		DBApp.createTable("n93m", cols1);
		for(int i=0;i<73;i++)
		{
			String [] record_n93m = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_n93m[j] = genRandString();
			}
			DBApp.insert("n93m", record_n93m);
		}
		int pagesCount_n93m = (int)Math.ceil(73.0/DBApp.dataPageSize);
		ArrayList<String> res_n93m = new ArrayList<>();
		for(int i=0;i<pagesCount_n93m;i++)
		{
			res_n93m.add(i+".db");
		}
		Collections.sort(res_n93m);
		String pagesTraceString_n93m = "n93m{ " + String.join(" ",res_n93m);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_n93m+")", fileManagerTrace1.contains(pagesTraceString_n93m));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test168TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"xe","hz","yh"};
		DBApp.createTable("f9", cols0);
		for(int i=0;i<48;i++)
		{
			String [] record_f9 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_f9[j] = genRandString();
			}
			DBApp.insert("f9", record_f9);
		}
		int pagesCount_f9 = (int)Math.ceil(48.0/DBApp.dataPageSize);
		ArrayList<String> res_f9 = new ArrayList<>();
		for(int i=0;i<pagesCount_f9;i++)
		{
			res_f9.add(i+".db");
		}
		Collections.sort(res_f9);
		String pagesTraceString_f9 = "f9{ " + String.join(" ",res_f9);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_f9+")", fileManagerTrace0.contains(pagesTraceString_f9));
		//--------------------------------------------------------------------------
		String[] cols1 = {"p11","hj"};
		DBApp.createTable("pu", cols1);
		for(int i=0;i<60;i++)
		{
			String [] record_pu = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_pu[j] = genRandString();
			}
			DBApp.insert("pu", record_pu);
		}
		int pagesCount_pu = (int)Math.ceil(60.0/DBApp.dataPageSize);
		ArrayList<String> res_pu = new ArrayList<>();
		for(int i=0;i<pagesCount_pu;i++)
		{
			res_pu.add(i+".db");
		}
		Collections.sort(res_pu);
		String pagesTraceString_pu = "pu{ " + String.join(" ",res_pu);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_pu+")", fileManagerTrace1.contains(pagesTraceString_pu));
		//--------------------------------------------------------------------------
		String[] cols2 = {"ly1","al9b","aur2","h4","e18","f9"};
		DBApp.createTable("vy", cols2);
		for(int i=0;i<20;i++)
		{
			String [] record_vy = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_vy[j] = genRandString();
			}
			DBApp.insert("vy", record_vy);
		}
		int pagesCount_vy = (int)Math.ceil(20.0/DBApp.dataPageSize);
		ArrayList<String> res_vy = new ArrayList<>();
		for(int i=0;i<pagesCount_vy;i++)
		{
			res_vy.add(i+".db");
		}
		Collections.sort(res_vy);
		String pagesTraceString_vy = "vy{ " + String.join(" ",res_vy);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_vy+")", fileManagerTrace2.contains(pagesTraceString_vy));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test169TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"md2","d4f","wq5v","h0","b3inz","op","d1","uiqy0","e11d","nd149"};
		DBApp.createTable("xm5", cols0);
		for(int i=0;i<96;i++)
		{
			String [] record_xm5 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_xm5[j] = genRandString();
			}
			DBApp.insert("xm5", record_xm5);
		}
		int pagesCount_xm5 = (int)Math.ceil(96.0/DBApp.dataPageSize);
		ArrayList<String> res_xm5 = new ArrayList<>();
		for(int i=0;i<pagesCount_xm5;i++)
		{
			res_xm5.add(i+".db");
		}
		Collections.sort(res_xm5);
		String pagesTraceString_xm5 = "xm5{ " + String.join(" ",res_xm5);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_xm5+")", fileManagerTrace0.contains(pagesTraceString_xm5));
		//--------------------------------------------------------------------------
		String[] cols1 = {"yznyt","e52kl"};
		DBApp.createTable("mjq4", cols1);
		for(int i=0;i<84;i++)
		{
			String [] record_mjq4 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_mjq4[j] = genRandString();
			}
			DBApp.insert("mjq4", record_mjq4);
		}
		int pagesCount_mjq4 = (int)Math.ceil(84.0/DBApp.dataPageSize);
		ArrayList<String> res_mjq4 = new ArrayList<>();
		for(int i=0;i<pagesCount_mjq4;i++)
		{
			res_mjq4.add(i+".db");
		}
		Collections.sort(res_mjq4);
		String pagesTraceString_mjq4 = "mjq4{ " + String.join(" ",res_mjq4);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_mjq4+")", fileManagerTrace1.contains(pagesTraceString_mjq4));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test0TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"yt5ux","ttn"};
		DBApp.createTable("a573c", cols0);
		String [][] records_a573c = new String[52][cols0.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_a573c[i][j] = genRandString();
			}
			DBApp.insert("a573c", records_a573c[i]);
			String tableTrace0 = DBApp.getLastTrace("a573c");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_a573c[i])+")", tableTrace0.contains(Arrays.toString(records_a573c[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"rwp","h1","oeb47","r9","a15","u00","a62","jcly0","pk1h"};
		DBApp.createTable("bd", cols1);
		String [][] records_bd = new String[34][cols1.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_bd[i][j] = genRandString();
			}
			DBApp.insert("bd", records_bd[i]);
			String tableTrace1 = DBApp.getLastTrace("bd");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_bd[i])+")", tableTrace1.contains(Arrays.toString(records_bd[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"od467","mx0hm","qw7","tz28"};
		DBApp.createTable("o0n", cols2);
		String [][] records_o0n = new String[58][cols2.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_o0n[i][j] = genRandString();
			}
			DBApp.insert("o0n", records_o0n[i]);
			String tableTrace2 = DBApp.getLastTrace("o0n");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_o0n[i])+")", tableTrace2.contains(Arrays.toString(records_o0n[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test1TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"bp10","o8","u607","me","no7","io2","h13v"};
		DBApp.createTable("li0p", cols0);
		String [][] records_li0p = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_li0p[i][j] = genRandString();
			}
			DBApp.insert("li0p", records_li0p[i]);
			String tableTrace0 = DBApp.getLastTrace("li0p");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_li0p[i])+")", tableTrace0.contains(Arrays.toString(records_li0p[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"q5375","e5sbk","pda5","u4s","rsr","ds"};
		DBApp.createTable("v3c", cols1);
		String [][] records_v3c = new String[37][cols1.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_v3c[i][j] = genRandString();
			}
			DBApp.insert("v3c", records_v3c[i]);
			String tableTrace1 = DBApp.getLastTrace("v3c");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_v3c[i])+")", tableTrace1.contains(Arrays.toString(records_v3c[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test2TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"n35o5","p12","mp8e","wxv"};
		DBApp.createTable("wip", cols0);
		String [][] records_wip = new String[28][cols0.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_wip[i][j] = genRandString();
			}
			DBApp.insert("wip", records_wip[i]);
			String tableTrace0 = DBApp.getLastTrace("wip");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_wip[i])+")", tableTrace0.contains(Arrays.toString(records_wip[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"e4fo","b9rl"};
		DBApp.createTable("njr", cols1);
		String [][] records_njr = new String[38][cols1.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_njr[i][j] = genRandString();
			}
			DBApp.insert("njr", records_njr[i]);
			String tableTrace1 = DBApp.getLastTrace("njr");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_njr[i])+")", tableTrace1.contains(Arrays.toString(records_njr[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"y221u","ym4","d6m","n06t","ma31","r8i7e","fuc","hh"};
		DBApp.createTable("v79", cols2);
		String [][] records_v79 = new String[2][cols2.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_v79[i][j] = genRandString();
			}
			DBApp.insert("v79", records_v79[i]);
			String tableTrace2 = DBApp.getLastTrace("v79");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_v79[i])+")", tableTrace2.contains(Arrays.toString(records_v79[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"kxg3","ofc","uw4","k6","bwb45","wf2","od"};
		DBApp.createTable("tm4", cols3);
		String [][] records_tm4 = new String[56][cols3.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_tm4[i][j] = genRandString();
			}
			DBApp.insert("tm4", records_tm4[i]);
			String tableTrace3 = DBApp.getLastTrace("tm4");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_tm4[i])+")", tableTrace3.contains(Arrays.toString(records_tm4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"ss","o1","k7d"};
		DBApp.createTable("rn1", cols4);
		String [][] records_rn1 = new String[11][cols4.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_rn1[i][j] = genRandString();
			}
			DBApp.insert("rn1", records_rn1[i]);
			String tableTrace4 = DBApp.getLastTrace("rn1");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_rn1[i])+")", tableTrace4.contains(Arrays.toString(records_rn1[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test3TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"t3v"};
		DBApp.createTable("glb", cols0);
		String [][] records_glb = new String[86][cols0.length];
		for(int i=0;i<86;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_glb[i][j] = genRandString();
			}
			DBApp.insert("glb", records_glb[i]);
			String tableTrace0 = DBApp.getLastTrace("glb");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_glb[i])+")", tableTrace0.contains(Arrays.toString(records_glb[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"s832","d3","wvl4","f39"};
		DBApp.createTable("jge", cols1);
		String [][] records_jge = new String[66][cols1.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_jge[i][j] = genRandString();
			}
			DBApp.insert("jge", records_jge[i]);
			String tableTrace1 = DBApp.getLastTrace("jge");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_jge[i])+")", tableTrace1.contains(Arrays.toString(records_jge[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"f8b","v47la","j874","ur1n","b5j"};
		DBApp.createTable("r6", cols2);
		String [][] records_r6 = new String[81][cols2.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_r6[i][j] = genRandString();
			}
			DBApp.insert("r6", records_r6[i]);
			String tableTrace2 = DBApp.getLastTrace("r6");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_r6[i])+")", tableTrace2.contains(Arrays.toString(records_r6[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"h4yt","en8","w25","i8","ti5","lxo"};
		DBApp.createTable("ldp", cols3);
		String [][] records_ldp = new String[14][cols3.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ldp[i][j] = genRandString();
			}
			DBApp.insert("ldp", records_ldp[i]);
			String tableTrace3 = DBApp.getLastTrace("ldp");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ldp[i])+")", tableTrace3.contains(Arrays.toString(records_ldp[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"vs2w","lx","r62f","gx"};
		DBApp.createTable("g0", cols4);
		String [][] records_g0 = new String[92][cols4.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_g0[i][j] = genRandString();
			}
			DBApp.insert("g0", records_g0[i]);
			String tableTrace4 = DBApp.getLastTrace("g0");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_g0[i])+")", tableTrace4.contains(Arrays.toString(records_g0[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test4TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"p9ca"};
		DBApp.createTable("kyf", cols0);
		String [][] records_kyf = new String[14][cols0.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_kyf[i][j] = genRandString();
			}
			DBApp.insert("kyf", records_kyf[i]);
			String tableTrace0 = DBApp.getLastTrace("kyf");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_kyf[i])+")", tableTrace0.contains(Arrays.toString(records_kyf[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"ps5","kv62","xp"};
		DBApp.createTable("gu2vw", cols1);
		String [][] records_gu2vw = new String[92][cols1.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_gu2vw[i][j] = genRandString();
			}
			DBApp.insert("gu2vw", records_gu2vw[i]);
			String tableTrace1 = DBApp.getLastTrace("gu2vw");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_gu2vw[i])+")", tableTrace1.contains(Arrays.toString(records_gu2vw[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"uq71p"};
		DBApp.createTable("p0z", cols2);
		String [][] records_p0z = new String[82][cols2.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_p0z[i][j] = genRandString();
			}
			DBApp.insert("p0z", records_p0z[i]);
			String tableTrace2 = DBApp.getLastTrace("p0z");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_p0z[i])+")", tableTrace2.contains(Arrays.toString(records_p0z[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"u53h","qiz1","r9"};
		DBApp.createTable("py5", cols3);
		String [][] records_py5 = new String[44][cols3.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_py5[i][j] = genRandString();
			}
			DBApp.insert("py5", records_py5[i]);
			String tableTrace3 = DBApp.getLastTrace("py5");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_py5[i])+")", tableTrace3.contains(Arrays.toString(records_py5[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test5TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"we","lkub","din"};
		DBApp.createTable("i2u", cols0);
		String [][] records_i2u = new String[95][cols0.length];
		for(int i=0;i<95;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i2u[i][j] = genRandString();
			}
			DBApp.insert("i2u", records_i2u[i]);
			String tableTrace0 = DBApp.getLastTrace("i2u");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_i2u[i])+")", tableTrace0.contains(Arrays.toString(records_i2u[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test6TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"iwc","h1l"};
		DBApp.createTable("i45", cols0);
		String [][] records_i45 = new String[48][cols0.length];
		for(int i=0;i<48;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i45[i][j] = genRandString();
			}
			DBApp.insert("i45", records_i45[i]);
			String tableTrace0 = DBApp.getLastTrace("i45");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_i45[i])+")", tableTrace0.contains(Arrays.toString(records_i45[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test7TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"m2n","uw","s18","l8g","l1j","h5t73","f79","z70n9"};
		DBApp.createTable("tj", cols0);
		String [][] records_tj = new String[76][cols0.length];
		for(int i=0;i<76;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_tj[i][j] = genRandString();
			}
			DBApp.insert("tj", records_tj[i]);
			String tableTrace0 = DBApp.getLastTrace("tj");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_tj[i])+")", tableTrace0.contains(Arrays.toString(records_tj[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test8TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"pl7a","n65","u2o3"};
		DBApp.createTable("ow", cols0);
		String [][] records_ow = new String[84][cols0.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ow[i][j] = genRandString();
			}
			DBApp.insert("ow", records_ow[i]);
			String tableTrace0 = DBApp.getLastTrace("ow");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ow[i])+")", tableTrace0.contains(Arrays.toString(records_ow[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test9TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"do66","d57t1","jc","u2i0","jj","i71a","j16","k4yeo","b77ze"};
		DBApp.createTable("n27e", cols0);
		String [][] records_n27e = new String[72][cols0.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_n27e[i][j] = genRandString();
			}
			DBApp.insert("n27e", records_n27e[i]);
			String tableTrace0 = DBApp.getLastTrace("n27e");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_n27e[i])+")", tableTrace0.contains(Arrays.toString(records_n27e[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"bhfj4","ka3s","yp0","s5xlw","l5e","p6k","o00","y96","vty","yz"};
		DBApp.createTable("hu", cols1);
		String [][] records_hu = new String[64][cols1.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_hu[i][j] = genRandString();
			}
			DBApp.insert("hu", records_hu[i]);
			String tableTrace1 = DBApp.getLastTrace("hu");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_hu[i])+")", tableTrace1.contains(Arrays.toString(records_hu[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test10TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"ug","fa16i","cd8i8","n1z","w78","tu5","r9y"};
		DBApp.createTable("op321", cols0);
		String [][] records_op321 = new String[64][cols0.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_op321[i][j] = genRandString();
			}
			DBApp.insert("op321", records_op321[i]);
			String tableTrace0 = DBApp.getLastTrace("op321");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_op321[i])+")", tableTrace0.contains(Arrays.toString(records_op321[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"nu53"};
		DBApp.createTable("vmz2l", cols1);
		String [][] records_vmz2l = new String[86][cols1.length];
		for(int i=0;i<86;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_vmz2l[i][j] = genRandString();
			}
			DBApp.insert("vmz2l", records_vmz2l[i]);
			String tableTrace1 = DBApp.getLastTrace("vmz2l");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_vmz2l[i])+")", tableTrace1.contains(Arrays.toString(records_vmz2l[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"u0g","r7","ox35","e9","m57","j5xg0","z4","le","p54"};
		DBApp.createTable("y1", cols2);
		String [][] records_y1 = new String[58][cols2.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_y1[i][j] = genRandString();
			}
			DBApp.insert("y1", records_y1[i]);
			String tableTrace2 = DBApp.getLastTrace("y1");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_y1[i])+")", tableTrace2.contains(Arrays.toString(records_y1[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test11TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"h7l","l92","z07","m1rn","n76","kr","u75"};
		DBApp.createTable("b081", cols0);
		String [][] records_b081 = new String[6][cols0.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_b081[i][j] = genRandString();
			}
			DBApp.insert("b081", records_b081[i]);
			String tableTrace0 = DBApp.getLastTrace("b081");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_b081[i])+")", tableTrace0.contains(Arrays.toString(records_b081[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"s5","angw2","f9tn","z82","ny1","t0v0r","xxc"};
		DBApp.createTable("yi", cols1);
		String [][] records_yi = new String[42][cols1.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_yi[i][j] = genRandString();
			}
			DBApp.insert("yi", records_yi[i]);
			String tableTrace1 = DBApp.getLastTrace("yi");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_yi[i])+")", tableTrace1.contains(Arrays.toString(records_yi[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"vhn1","ipec"};
		DBApp.createTable("f8o4f", cols2);
		String [][] records_f8o4f = new String[18][cols2.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_f8o4f[i][j] = genRandString();
			}
			DBApp.insert("f8o4f", records_f8o4f[i]);
			String tableTrace2 = DBApp.getLastTrace("f8o4f");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_f8o4f[i])+")", tableTrace2.contains(Arrays.toString(records_f8o4f[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test12TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"w1","is0ac","rss","l45","b8","p4f0b","j2","p4","z4","tvi2o"};
		DBApp.createTable("c5r3", cols0);
		String [][] records_c5r3 = new String[89][cols0.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_c5r3[i][j] = genRandString();
			}
			DBApp.insert("c5r3", records_c5r3[i]);
			String tableTrace0 = DBApp.getLastTrace("c5r3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_c5r3[i])+")", tableTrace0.contains(Arrays.toString(records_c5r3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"ay3","qy"};
		DBApp.createTable("tf5m", cols1);
		String [][] records_tf5m = new String[69][cols1.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_tf5m[i][j] = genRandString();
			}
			DBApp.insert("tf5m", records_tf5m[i]);
			String tableTrace1 = DBApp.getLastTrace("tf5m");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_tf5m[i])+")", tableTrace1.contains(Arrays.toString(records_tf5m[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"lox","e5v","e42","m1o","k63"};
		DBApp.createTable("c45", cols2);
		String [][] records_c45 = new String[75][cols2.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_c45[i][j] = genRandString();
			}
			DBApp.insert("c45", records_c45[i]);
			String tableTrace2 = DBApp.getLastTrace("c45");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_c45[i])+")", tableTrace2.contains(Arrays.toString(records_c45[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"iq3j6","e2c","lwn","ul"};
		DBApp.createTable("e5uu6", cols3);
		String [][] records_e5uu6 = new String[99][cols3.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_e5uu6[i][j] = genRandString();
			}
			DBApp.insert("e5uu6", records_e5uu6[i]);
			String tableTrace3 = DBApp.getLastTrace("e5uu6");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_e5uu6[i])+")", tableTrace3.contains(Arrays.toString(records_e5uu6[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test13TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"vi1","t7439","m6e"};
		DBApp.createTable("ofo", cols0);
		String [][] records_ofo = new String[18][cols0.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ofo[i][j] = genRandString();
			}
			DBApp.insert("ofo", records_ofo[i]);
			String tableTrace0 = DBApp.getLastTrace("ofo");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ofo[i])+")", tableTrace0.contains(Arrays.toString(records_ofo[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"i4d","c8063","liz","hr611","dt","w80y5","ab","e76"};
		DBApp.createTable("f259", cols1);
		String [][] records_f259 = new String[62][cols1.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_f259[i][j] = genRandString();
			}
			DBApp.insert("f259", records_f259[i]);
			String tableTrace1 = DBApp.getLastTrace("f259");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_f259[i])+")", tableTrace1.contains(Arrays.toString(records_f259[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"tx","q5ayy","l4","a2b","q7l","gwq"};
		DBApp.createTable("g3", cols2);
		String [][] records_g3 = new String[64][cols2.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_g3[i][j] = genRandString();
			}
			DBApp.insert("g3", records_g3[i]);
			String tableTrace2 = DBApp.getLastTrace("g3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_g3[i])+")", tableTrace2.contains(Arrays.toString(records_g3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"hf","o63","u67q0","jje63","cex","j33"};
		DBApp.createTable("hj0", cols3);
		String [][] records_hj0 = new String[59][cols3.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_hj0[i][j] = genRandString();
			}
			DBApp.insert("hj0", records_hj0[i]);
			String tableTrace3 = DBApp.getLastTrace("hj0");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_hj0[i])+")", tableTrace3.contains(Arrays.toString(records_hj0[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test14TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"i32","zx6","aet"};
		DBApp.createTable("u94", cols0);
		String [][] records_u94 = new String[77][cols0.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u94[i][j] = genRandString();
			}
			DBApp.insert("u94", records_u94[i]);
			String tableTrace0 = DBApp.getLastTrace("u94");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_u94[i])+")", tableTrace0.contains(Arrays.toString(records_u94[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"y92qd"};
		DBApp.createTable("j0", cols1);
		String [][] records_j0 = new String[46][cols1.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_j0[i][j] = genRandString();
			}
			DBApp.insert("j0", records_j0[i]);
			String tableTrace1 = DBApp.getLastTrace("j0");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_j0[i])+")", tableTrace1.contains(Arrays.toString(records_j0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"u1w","j1","s09e","a77","l12","uo"};
		DBApp.createTable("zsd7", cols2);
		String [][] records_zsd7 = new String[56][cols2.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_zsd7[i][j] = genRandString();
			}
			DBApp.insert("zsd7", records_zsd7[i]);
			String tableTrace2 = DBApp.getLastTrace("zsd7");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_zsd7[i])+")", tableTrace2.contains(Arrays.toString(records_zsd7[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test15TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"mf814","a413","t9s","wmq","s76","v5q9","ml"};
		DBApp.createTable("m2p", cols0);
		String [][] records_m2p = new String[19][cols0.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_m2p[i][j] = genRandString();
			}
			DBApp.insert("m2p", records_m2p[i]);
			String tableTrace0 = DBApp.getLastTrace("m2p");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_m2p[i])+")", tableTrace0.contains(Arrays.toString(records_m2p[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"gu7","h8sp5","dk","ye","ws8","t95","m71","jyq2","ahsx","j3cf1"};
		DBApp.createTable("c4w4", cols1);
		String [][] records_c4w4 = new String[81][cols1.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_c4w4[i][j] = genRandString();
			}
			DBApp.insert("c4w4", records_c4w4[i]);
			String tableTrace1 = DBApp.getLastTrace("c4w4");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_c4w4[i])+")", tableTrace1.contains(Arrays.toString(records_c4w4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"x9x","cw","n0","grhbr","kv"};
		DBApp.createTable("yft", cols2);
		String [][] records_yft = new String[71][cols2.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_yft[i][j] = genRandString();
			}
			DBApp.insert("yft", records_yft[i]);
			String tableTrace2 = DBApp.getLastTrace("yft");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_yft[i])+")", tableTrace2.contains(Arrays.toString(records_yft[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"kft"};
		DBApp.createTable("a6141", cols3);
		String [][] records_a6141 = new String[6][cols3.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_a6141[i][j] = genRandString();
			}
			DBApp.insert("a6141", records_a6141[i]);
			String tableTrace3 = DBApp.getLastTrace("a6141");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_a6141[i])+")", tableTrace3.contains(Arrays.toString(records_a6141[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test16TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"zu4","c2","uiu","nu","a3u2k"};
		DBApp.createTable("horf", cols0);
		String [][] records_horf = new String[58][cols0.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_horf[i][j] = genRandString();
			}
			DBApp.insert("horf", records_horf[i]);
			String tableTrace0 = DBApp.getLastTrace("horf");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_horf[i])+")", tableTrace0.contains(Arrays.toString(records_horf[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"q6b","jpn","xd47b","w7","c9o","c4","nj4","z954e","d92xf","bc33"};
		DBApp.createTable("uui", cols1);
		String [][] records_uui = new String[91][cols1.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_uui[i][j] = genRandString();
			}
			DBApp.insert("uui", records_uui[i]);
			String tableTrace1 = DBApp.getLastTrace("uui");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_uui[i])+")", tableTrace1.contains(Arrays.toString(records_uui[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"lc4b6","i06b","z6z3","o15","s9"};
		DBApp.createTable("v86", cols2);
		String [][] records_v86 = new String[4][cols2.length];
		for(int i=0;i<4;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_v86[i][j] = genRandString();
			}
			DBApp.insert("v86", records_v86[i]);
			String tableTrace2 = DBApp.getLastTrace("v86");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_v86[i])+")", tableTrace2.contains(Arrays.toString(records_v86[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"zx3","pks0","z96l7","kq","s4","iasq","jybp","ml","a4l1g","p9r"};
		DBApp.createTable("dr", cols3);
		String [][] records_dr = new String[18][cols3.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_dr[i][j] = genRandString();
			}
			DBApp.insert("dr", records_dr[i]);
			String tableTrace3 = DBApp.getLastTrace("dr");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_dr[i])+")", tableTrace3.contains(Arrays.toString(records_dr[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test17TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"olr"};
		DBApp.createTable("a26na", cols0);
		String [][] records_a26na = new String[86][cols0.length];
		for(int i=0;i<86;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_a26na[i][j] = genRandString();
			}
			DBApp.insert("a26na", records_a26na[i]);
			String tableTrace0 = DBApp.getLastTrace("a26na");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_a26na[i])+")", tableTrace0.contains(Arrays.toString(records_a26na[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"hadl","ku"};
		DBApp.createTable("lz7m", cols1);
		String [][] records_lz7m = new String[11][cols1.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_lz7m[i][j] = genRandString();
			}
			DBApp.insert("lz7m", records_lz7m[i]);
			String tableTrace1 = DBApp.getLastTrace("lz7m");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_lz7m[i])+")", tableTrace1.contains(Arrays.toString(records_lz7m[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test18TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"hzf2v"};
		DBApp.createTable("ooi2f", cols0);
		String [][] records_ooi2f = new String[87][cols0.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ooi2f[i][j] = genRandString();
			}
			DBApp.insert("ooi2f", records_ooi2f[i]);
			String tableTrace0 = DBApp.getLastTrace("ooi2f");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ooi2f[i])+")", tableTrace0.contains(Arrays.toString(records_ooi2f[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"q4o0","fyx","g5079","u51","s5r2i","zqm"};
		DBApp.createTable("r3", cols1);
		String [][] records_r3 = new String[1][cols1.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_r3[i][j] = genRandString();
			}
			DBApp.insert("r3", records_r3[i]);
			String tableTrace1 = DBApp.getLastTrace("r3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_r3[i])+")", tableTrace1.contains(Arrays.toString(records_r3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"o7","u308","g12","cxx62","nr3l","n5x5","u3y8u","w4","fl7"};
		DBApp.createTable("e8e", cols2);
		String [][] records_e8e = new String[88][cols2.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_e8e[i][j] = genRandString();
			}
			DBApp.insert("e8e", records_e8e[i]);
			String tableTrace2 = DBApp.getLastTrace("e8e");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_e8e[i])+")", tableTrace2.contains(Arrays.toString(records_e8e[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"hn0","wdzo","s22j7","amj","pr4","y24"};
		DBApp.createTable("n7y", cols3);
		String [][] records_n7y = new String[1][cols3.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_n7y[i][j] = genRandString();
			}
			DBApp.insert("n7y", records_n7y[i]);
			String tableTrace3 = DBApp.getLastTrace("n7y");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_n7y[i])+")", tableTrace3.contains(Arrays.toString(records_n7y[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test19TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"ot","c7hj","f7i"};
		DBApp.createTable("bo", cols0);
		String [][] records_bo = new String[1][cols0.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_bo[i][j] = genRandString();
			}
			DBApp.insert("bo", records_bo[i]);
			String tableTrace0 = DBApp.getLastTrace("bo");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_bo[i])+")", tableTrace0.contains(Arrays.toString(records_bo[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"c2936","ta652"};
		DBApp.createTable("a8f", cols1);
		String [][] records_a8f = new String[74][cols1.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_a8f[i][j] = genRandString();
			}
			DBApp.insert("a8f", records_a8f[i]);
			String tableTrace1 = DBApp.getLastTrace("a8f");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_a8f[i])+")", tableTrace1.contains(Arrays.toString(records_a8f[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"mov","o31","egn","uei9","l58q","r4i","i78","ph4ib","qvy"};
		DBApp.createTable("i98w5", cols2);
		String [][] records_i98w5 = new String[59][cols2.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_i98w5[i][j] = genRandString();
			}
			DBApp.insert("i98w5", records_i98w5[i]);
			String tableTrace2 = DBApp.getLastTrace("i98w5");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_i98w5[i])+")", tableTrace2.contains(Arrays.toString(records_i98w5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"oak","n5p","zac","g8","qt6"};
		DBApp.createTable("te6", cols3);
		String [][] records_te6 = new String[73][cols3.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_te6[i][j] = genRandString();
			}
			DBApp.insert("te6", records_te6[i]);
			String tableTrace3 = DBApp.getLastTrace("te6");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_te6[i])+")", tableTrace3.contains(Arrays.toString(records_te6[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test20TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"rf","hncas","hp0","r950","yes","wv","o04","p9"};
		DBApp.createTable("sd7r", cols0);
		String [][] records_sd7r = new String[56][cols0.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sd7r[i][j] = genRandString();
			}
			DBApp.insert("sd7r", records_sd7r[i]);
			String tableTrace0 = DBApp.getLastTrace("sd7r");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_sd7r[i])+")", tableTrace0.contains(Arrays.toString(records_sd7r[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"vwu","c6f1","yg32v","d3z","u1s","o8f","j13u","iu","lq5v","nbu"};
		DBApp.createTable("a5z", cols1);
		String [][] records_a5z = new String[87][cols1.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_a5z[i][j] = genRandString();
			}
			DBApp.insert("a5z", records_a5z[i]);
			String tableTrace1 = DBApp.getLastTrace("a5z");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_a5z[i])+")", tableTrace1.contains(Arrays.toString(records_a5z[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"dmy","f73"};
		DBApp.createTable("v212r", cols2);
		String [][] records_v212r = new String[100][cols2.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_v212r[i][j] = genRandString();
			}
			DBApp.insert("v212r", records_v212r[i]);
			String tableTrace2 = DBApp.getLastTrace("v212r");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_v212r[i])+")", tableTrace2.contains(Arrays.toString(records_v212r[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"j8","s2","h9","p7mhb"};
		DBApp.createTable("bb8", cols3);
		String [][] records_bb8 = new String[20][cols3.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_bb8[i][j] = genRandString();
			}
			DBApp.insert("bb8", records_bb8[i]);
			String tableTrace3 = DBApp.getLastTrace("bb8");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_bb8[i])+")", tableTrace3.contains(Arrays.toString(records_bb8[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"psqc","j9","q8","d8u71","yc","q6zp","sy0q","oph","id"};
		DBApp.createTable("e2r", cols4);
		String [][] records_e2r = new String[13][cols4.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_e2r[i][j] = genRandString();
			}
			DBApp.insert("e2r", records_e2r[i]);
			String tableTrace4 = DBApp.getLastTrace("e2r");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_e2r[i])+")", tableTrace4.contains(Arrays.toString(records_e2r[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test21TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"hsi","yd","r0z9","f6","y9","y3s","y2s","e1"};
		DBApp.createTable("qo", cols0);
		String [][] records_qo = new String[42][cols0.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_qo[i][j] = genRandString();
			}
			DBApp.insert("qo", records_qo[i]);
			String tableTrace0 = DBApp.getLastTrace("qo");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_qo[i])+")", tableTrace0.contains(Arrays.toString(records_qo[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test22TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"x1n","qv","vx9n","m5s","g5su7","c9","x7","ut5n0"};
		DBApp.createTable("h77", cols0);
		String [][] records_h77 = new String[68][cols0.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h77[i][j] = genRandString();
			}
			DBApp.insert("h77", records_h77[i]);
			String tableTrace0 = DBApp.getLastTrace("h77");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_h77[i])+")", tableTrace0.contains(Arrays.toString(records_h77[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"w67f","q65","d5f36","ne91","v48q","gj1"};
		DBApp.createTable("zl", cols1);
		String [][] records_zl = new String[78][cols1.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_zl[i][j] = genRandString();
			}
			DBApp.insert("zl", records_zl[i]);
			String tableTrace1 = DBApp.getLastTrace("zl");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_zl[i])+")", tableTrace1.contains(Arrays.toString(records_zl[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test23TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"niu","f4z","ko7","mqy","cp"};
		DBApp.createTable("u438", cols0);
		String [][] records_u438 = new String[52][cols0.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u438[i][j] = genRandString();
			}
			DBApp.insert("u438", records_u438[i]);
			String tableTrace0 = DBApp.getLastTrace("u438");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_u438[i])+")", tableTrace0.contains(Arrays.toString(records_u438[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"oxky","chhr","e3a","oq","sl","nlw","mg"};
		DBApp.createTable("gd", cols1);
		String [][] records_gd = new String[85][cols1.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_gd[i][j] = genRandString();
			}
			DBApp.insert("gd", records_gd[i]);
			String tableTrace1 = DBApp.getLastTrace("gd");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_gd[i])+")", tableTrace1.contains(Arrays.toString(records_gd[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"rywv5","yjgv","ud","g32","c1","f00f7","rb1k","d51q4","em"};
		DBApp.createTable("z3", cols2);
		String [][] records_z3 = new String[72][cols2.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_z3[i][j] = genRandString();
			}
			DBApp.insert("z3", records_z3[i]);
			String tableTrace2 = DBApp.getLastTrace("z3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_z3[i])+")", tableTrace2.contains(Arrays.toString(records_z3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"n07gg","dkp","m5o69","f7f","wf","zi","t4","iwc8","xjm17"};
		DBApp.createTable("o80ph", cols3);
		String [][] records_o80ph = new String[65][cols3.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_o80ph[i][j] = genRandString();
			}
			DBApp.insert("o80ph", records_o80ph[i]);
			String tableTrace3 = DBApp.getLastTrace("o80ph");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_o80ph[i])+")", tableTrace3.contains(Arrays.toString(records_o80ph[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test24TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"j6o9","ns1","jwt","vo","k1t","t3z7t"};
		DBApp.createTable("q7o", cols0);
		String [][] records_q7o = new String[45][cols0.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_q7o[i][j] = genRandString();
			}
			DBApp.insert("q7o", records_q7o[i]);
			String tableTrace0 = DBApp.getLastTrace("q7o");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_q7o[i])+")", tableTrace0.contains(Arrays.toString(records_q7o[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"dz","s3z","a03s","vl","a7y","yp1","te7q2","qz0","f35y","s6g82"};
		DBApp.createTable("cp", cols1);
		String [][] records_cp = new String[21][cols1.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_cp[i][j] = genRandString();
			}
			DBApp.insert("cp", records_cp[i]);
			String tableTrace1 = DBApp.getLastTrace("cp");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_cp[i])+")", tableTrace1.contains(Arrays.toString(records_cp[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"j178","afc","ii","o830","g5","r5s2b","dpn3","eo8h","m3sho","j1je"};
		DBApp.createTable("z9l", cols2);
		String [][] records_z9l = new String[85][cols2.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_z9l[i][j] = genRandString();
			}
			DBApp.insert("z9l", records_z9l[i]);
			String tableTrace2 = DBApp.getLastTrace("z9l");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_z9l[i])+")", tableTrace2.contains(Arrays.toString(records_z9l[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test25TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"t1rt6","hr5","q2ujk","i2","mzwki","eg"};
		DBApp.createTable("l2n", cols0);
		String [][] records_l2n = new String[38][cols0.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_l2n[i][j] = genRandString();
			}
			DBApp.insert("l2n", records_l2n[i]);
			String tableTrace0 = DBApp.getLastTrace("l2n");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_l2n[i])+")", tableTrace0.contains(Arrays.toString(records_l2n[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"ou","ym8","pq","m6dij","wg1"};
		DBApp.createTable("l9l", cols1);
		String [][] records_l9l = new String[68][cols1.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_l9l[i][j] = genRandString();
			}
			DBApp.insert("l9l", records_l9l[i]);
			String tableTrace1 = DBApp.getLastTrace("l9l");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_l9l[i])+")", tableTrace1.contains(Arrays.toString(records_l9l[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test26TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"im82","zt","vn","tpn8l","j48z1","m7g5","h55s9"};
		DBApp.createTable("s904w", cols0);
		String [][] records_s904w = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_s904w[i][j] = genRandString();
			}
			DBApp.insert("s904w", records_s904w[i]);
			String tableTrace0 = DBApp.getLastTrace("s904w");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_s904w[i])+")", tableTrace0.contains(Arrays.toString(records_s904w[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"c6bni","p02","kh56g","leb","b9","jg","gy7","yj2"};
		DBApp.createTable("mr", cols1);
		String [][] records_mr = new String[21][cols1.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_mr[i][j] = genRandString();
			}
			DBApp.insert("mr", records_mr[i]);
			String tableTrace1 = DBApp.getLastTrace("mr");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_mr[i])+")", tableTrace1.contains(Arrays.toString(records_mr[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"laga1"};
		DBApp.createTable("k6m1v", cols2);
		String [][] records_k6m1v = new String[88][cols2.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_k6m1v[i][j] = genRandString();
			}
			DBApp.insert("k6m1v", records_k6m1v[i]);
			String tableTrace2 = DBApp.getLastTrace("k6m1v");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_k6m1v[i])+")", tableTrace2.contains(Arrays.toString(records_k6m1v[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"h4"};
		DBApp.createTable("q82", cols3);
		String [][] records_q82 = new String[60][cols3.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_q82[i][j] = genRandString();
			}
			DBApp.insert("q82", records_q82[i]);
			String tableTrace3 = DBApp.getLastTrace("q82");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_q82[i])+")", tableTrace3.contains(Arrays.toString(records_q82[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"fkwbd","yg7pr"};
		DBApp.createTable("dn2", cols4);
		String [][] records_dn2 = new String[5][cols4.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_dn2[i][j] = genRandString();
			}
			DBApp.insert("dn2", records_dn2[i]);
			String tableTrace4 = DBApp.getLastTrace("dn2");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_dn2[i])+")", tableTrace4.contains(Arrays.toString(records_dn2[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test27TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"x9s47","s1","e3zc","w5","o44","rd8d","o09","neq2","k3i5"};
		DBApp.createTable("k7x", cols0);
		String [][] records_k7x = new String[40][cols0.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k7x[i][j] = genRandString();
			}
			DBApp.insert("k7x", records_k7x[i]);
			String tableTrace0 = DBApp.getLastTrace("k7x");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_k7x[i])+")", tableTrace0.contains(Arrays.toString(records_k7x[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"i1m","l2j","oo6h","t8ko","ag","x57u"};
		DBApp.createTable("av", cols1);
		String [][] records_av = new String[70][cols1.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_av[i][j] = genRandString();
			}
			DBApp.insert("av", records_av[i]);
			String tableTrace1 = DBApp.getLastTrace("av");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_av[i])+")", tableTrace1.contains(Arrays.toString(records_av[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test28TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"qf2","j26e1","o24"};
		DBApp.createTable("vo30", cols0);
		String [][] records_vo30 = new String[14][cols0.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vo30[i][j] = genRandString();
			}
			DBApp.insert("vo30", records_vo30[i]);
			String tableTrace0 = DBApp.getLastTrace("vo30");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_vo30[i])+")", tableTrace0.contains(Arrays.toString(records_vo30[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"vn648"};
		DBApp.createTable("ib1", cols1);
		String [][] records_ib1 = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ib1[i][j] = genRandString();
			}
			DBApp.insert("ib1", records_ib1[i]);
			String tableTrace1 = DBApp.getLastTrace("ib1");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ib1[i])+")", tableTrace1.contains(Arrays.toString(records_ib1[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"qi9f3","d58","sp","v63","pnbvx","j4xk","id7","jkcwx"};
		DBApp.createTable("yk7", cols2);
		String [][] records_yk7 = new String[32][cols2.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_yk7[i][j] = genRandString();
			}
			DBApp.insert("yk7", records_yk7[i]);
			String tableTrace2 = DBApp.getLastTrace("yk7");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_yk7[i])+")", tableTrace2.contains(Arrays.toString(records_yk7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"ubk6d","i7k","i1p","v7z","ssea","zk1","w04","my4","q945"};
		DBApp.createTable("lm88", cols3);
		String [][] records_lm88 = new String[59][cols3.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_lm88[i][j] = genRandString();
			}
			DBApp.insert("lm88", records_lm88[i]);
			String tableTrace3 = DBApp.getLastTrace("lm88");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_lm88[i])+")", tableTrace3.contains(Arrays.toString(records_lm88[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"jck","e21"};
		DBApp.createTable("d2g", cols4);
		String [][] records_d2g = new String[52][cols4.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_d2g[i][j] = genRandString();
			}
			DBApp.insert("d2g", records_d2g[i]);
			String tableTrace4 = DBApp.getLastTrace("d2g");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_d2g[i])+")", tableTrace4.contains(Arrays.toString(records_d2g[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test29TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"ce09"};
		DBApp.createTable("dv2", cols0);
		String [][] records_dv2 = new String[21][cols0.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_dv2[i][j] = genRandString();
			}
			DBApp.insert("dv2", records_dv2[i]);
			String tableTrace0 = DBApp.getLastTrace("dv2");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_dv2[i])+")", tableTrace0.contains(Arrays.toString(records_dv2[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"z4c","vb","ehi24","vw"};
		DBApp.createTable("k2ig9", cols1);
		String [][] records_k2ig9 = new String[93][cols1.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_k2ig9[i][j] = genRandString();
			}
			DBApp.insert("k2ig9", records_k2ig9[i]);
			String tableTrace1 = DBApp.getLastTrace("k2ig9");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_k2ig9[i])+")", tableTrace1.contains(Arrays.toString(records_k2ig9[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test30TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"d5","sz2jh","h58","erg","j54n","wy6h","j96","b332c"};
		DBApp.createTable("pi627", cols0);
		String [][] records_pi627 = new String[75][cols0.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_pi627[i][j] = genRandString();
			}
			DBApp.insert("pi627", records_pi627[i]);
			String tableTrace0 = DBApp.getLastTrace("pi627");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_pi627[i])+")", tableTrace0.contains(Arrays.toString(records_pi627[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test31TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"zx7k4","bqki","in5"};
		DBApp.createTable("cer", cols0);
		String [][] records_cer = new String[42][cols0.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_cer[i][j] = genRandString();
			}
			DBApp.insert("cer", records_cer[i]);
			String tableTrace0 = DBApp.getLastTrace("cer");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_cer[i])+")", tableTrace0.contains(Arrays.toString(records_cer[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"i282","c6","il","d0p","x03","g00","f5763"};
		DBApp.createTable("l7d", cols1);
		String [][] records_l7d = new String[47][cols1.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_l7d[i][j] = genRandString();
			}
			DBApp.insert("l7d", records_l7d[i]);
			String tableTrace1 = DBApp.getLastTrace("l7d");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_l7d[i])+")", tableTrace1.contains(Arrays.toString(records_l7d[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"kk16m","r2wz","zwi","zxiq6","b1vt","myys3","h6u"};
		DBApp.createTable("ls", cols2);
		String [][] records_ls = new String[66][cols2.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ls[i][j] = genRandString();
			}
			DBApp.insert("ls", records_ls[i]);
			String tableTrace2 = DBApp.getLastTrace("ls");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ls[i])+")", tableTrace2.contains(Arrays.toString(records_ls[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a5w","yt","jn1","b1","nr"};
		DBApp.createTable("f0o", cols3);
		String [][] records_f0o = new String[79][cols3.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_f0o[i][j] = genRandString();
			}
			DBApp.insert("f0o", records_f0o[i]);
			String tableTrace3 = DBApp.getLastTrace("f0o");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_f0o[i])+")", tableTrace3.contains(Arrays.toString(records_f0o[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test32TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"v5lbq","zdnx","n29","n6"};
		DBApp.createTable("b2y", cols0);
		String [][] records_b2y = new String[81][cols0.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_b2y[i][j] = genRandString();
			}
			DBApp.insert("b2y", records_b2y[i]);
			String tableTrace0 = DBApp.getLastTrace("b2y");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_b2y[i])+")", tableTrace0.contains(Arrays.toString(records_b2y[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test33TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"gl0qz","f5","s2","i9","kd","ac","b7x","v9a","cytv","t3a"};
		DBApp.createTable("k2m", cols0);
		String [][] records_k2m = new String[34][cols0.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k2m[i][j] = genRandString();
			}
			DBApp.insert("k2m", records_k2m[i]);
			String tableTrace0 = DBApp.getLastTrace("k2m");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_k2m[i])+")", tableTrace0.contains(Arrays.toString(records_k2m[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"ty","tnv"};
		DBApp.createTable("u7", cols1);
		String [][] records_u7 = new String[92][cols1.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_u7[i][j] = genRandString();
			}
			DBApp.insert("u7", records_u7[i]);
			String tableTrace1 = DBApp.getLastTrace("u7");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_u7[i])+")", tableTrace1.contains(Arrays.toString(records_u7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"o425","q6k","lf75","v9o","wp51w","zl","pub","y4x","ik9","u6"};
		DBApp.createTable("g43", cols2);
		String [][] records_g43 = new String[27][cols2.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_g43[i][j] = genRandString();
			}
			DBApp.insert("g43", records_g43[i]);
			String tableTrace2 = DBApp.getLastTrace("g43");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_g43[i])+")", tableTrace2.contains(Arrays.toString(records_g43[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"jb","z2m"};
		DBApp.createTable("pa", cols3);
		String [][] records_pa = new String[32][cols3.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_pa[i][j] = genRandString();
			}
			DBApp.insert("pa", records_pa[i]);
			String tableTrace3 = DBApp.getLastTrace("pa");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_pa[i])+")", tableTrace3.contains(Arrays.toString(records_pa[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"b45l","ng","o7h","jzr","c6463","roly6"};
		DBApp.createTable("c9", cols4);
		String [][] records_c9 = new String[78][cols4.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_c9[i][j] = genRandString();
			}
			DBApp.insert("c9", records_c9[i]);
			String tableTrace4 = DBApp.getLastTrace("c9");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_c9[i])+")", tableTrace4.contains(Arrays.toString(records_c9[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test34TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"m1","t9y6m","cze4","ira","ao9w","f4h"};
		DBApp.createTable("vpd4z", cols0);
		String [][] records_vpd4z = new String[11][cols0.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vpd4z[i][j] = genRandString();
			}
			DBApp.insert("vpd4z", records_vpd4z[i]);
			String tableTrace0 = DBApp.getLastTrace("vpd4z");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_vpd4z[i])+")", tableTrace0.contains(Arrays.toString(records_vpd4z[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"dtd"};
		DBApp.createTable("wx4", cols1);
		String [][] records_wx4 = new String[21][cols1.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_wx4[i][j] = genRandString();
			}
			DBApp.insert("wx4", records_wx4[i]);
			String tableTrace1 = DBApp.getLastTrace("wx4");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_wx4[i])+")", tableTrace1.contains(Arrays.toString(records_wx4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"nvt"};
		DBApp.createTable("i1v", cols2);
		String [][] records_i1v = new String[1][cols2.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_i1v[i][j] = genRandString();
			}
			DBApp.insert("i1v", records_i1v[i]);
			String tableTrace2 = DBApp.getLastTrace("i1v");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_i1v[i])+")", tableTrace2.contains(Arrays.toString(records_i1v[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test35TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"el6o5","i9b","u46","lce3","vm59","a79","e99","gy9","xzt"};
		DBApp.createTable("pfg", cols0);
		String [][] records_pfg = new String[92][cols0.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_pfg[i][j] = genRandString();
			}
			DBApp.insert("pfg", records_pfg[i]);
			String tableTrace0 = DBApp.getLastTrace("pfg");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_pfg[i])+")", tableTrace0.contains(Arrays.toString(records_pfg[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"pnh","bfw","f88","bej","h3r","g4o"};
		DBApp.createTable("g45", cols1);
		String [][] records_g45 = new String[56][cols1.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_g45[i][j] = genRandString();
			}
			DBApp.insert("g45", records_g45[i]);
			String tableTrace1 = DBApp.getLastTrace("g45");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_g45[i])+")", tableTrace1.contains(Arrays.toString(records_g45[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"uj9d"};
		DBApp.createTable("n6g", cols2);
		String [][] records_n6g = new String[90][cols2.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_n6g[i][j] = genRandString();
			}
			DBApp.insert("n6g", records_n6g[i]);
			String tableTrace2 = DBApp.getLastTrace("n6g");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_n6g[i])+")", tableTrace2.contains(Arrays.toString(records_n6g[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test36TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"u1a","yb"};
		DBApp.createTable("wb0", cols0);
		String [][] records_wb0 = new String[47][cols0.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_wb0[i][j] = genRandString();
			}
			DBApp.insert("wb0", records_wb0[i]);
			String tableTrace0 = DBApp.getLastTrace("wb0");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_wb0[i])+")", tableTrace0.contains(Arrays.toString(records_wb0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"g13d3","bzb22","b5"};
		DBApp.createTable("o5c3", cols1);
		String [][] records_o5c3 = new String[35][cols1.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_o5c3[i][j] = genRandString();
			}
			DBApp.insert("o5c3", records_o5c3[i]);
			String tableTrace1 = DBApp.getLastTrace("o5c3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_o5c3[i])+")", tableTrace1.contains(Arrays.toString(records_o5c3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"j37","g776"};
		DBApp.createTable("bd6x", cols2);
		String [][] records_bd6x = new String[64][cols2.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_bd6x[i][j] = genRandString();
			}
			DBApp.insert("bd6x", records_bd6x[i]);
			String tableTrace2 = DBApp.getLastTrace("bd6x");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_bd6x[i])+")", tableTrace2.contains(Arrays.toString(records_bd6x[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"xcvt","ar3","cr","z29","j80","paa4","xbv","rqh","gtt4","z2k"};
		DBApp.createTable("pg29", cols3);
		String [][] records_pg29 = new String[92][cols3.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_pg29[i][j] = genRandString();
			}
			DBApp.insert("pg29", records_pg29[i]);
			String tableTrace3 = DBApp.getLastTrace("pg29");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_pg29[i])+")", tableTrace3.contains(Arrays.toString(records_pg29[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test37TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"y9v2","ig14","o20"};
		DBApp.createTable("oc", cols0);
		String [][] records_oc = new String[2][cols0.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_oc[i][j] = genRandString();
			}
			DBApp.insert("oc", records_oc[i]);
			String tableTrace0 = DBApp.getLastTrace("oc");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_oc[i])+")", tableTrace0.contains(Arrays.toString(records_oc[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"vf5","lzx","cx","xj507","yea","w49","mp95","j503","v7g","f49"};
		DBApp.createTable("eu0", cols1);
		String [][] records_eu0 = new String[23][cols1.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_eu0[i][j] = genRandString();
			}
			DBApp.insert("eu0", records_eu0[i]);
			String tableTrace1 = DBApp.getLastTrace("eu0");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_eu0[i])+")", tableTrace1.contains(Arrays.toString(records_eu0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"ryjq"};
		DBApp.createTable("hlwo", cols2);
		String [][] records_hlwo = new String[16][cols2.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_hlwo[i][j] = genRandString();
			}
			DBApp.insert("hlwo", records_hlwo[i]);
			String tableTrace2 = DBApp.getLastTrace("hlwo");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_hlwo[i])+")", tableTrace2.contains(Arrays.toString(records_hlwo[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"kfe","e4588","lfi0w","tw5","gtx84"};
		DBApp.createTable("qu3", cols3);
		String [][] records_qu3 = new String[11][cols3.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_qu3[i][j] = genRandString();
			}
			DBApp.insert("qu3", records_qu3[i]);
			String tableTrace3 = DBApp.getLastTrace("qu3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_qu3[i])+")", tableTrace3.contains(Arrays.toString(records_qu3[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test38TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"h1q","o04y4","t21r","q1","z1","ydh43","ll2"};
		DBApp.createTable("he29", cols0);
		String [][] records_he29 = new String[29][cols0.length];
		for(int i=0;i<29;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_he29[i][j] = genRandString();
			}
			DBApp.insert("he29", records_he29[i]);
			String tableTrace0 = DBApp.getLastTrace("he29");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_he29[i])+")", tableTrace0.contains(Arrays.toString(records_he29[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"zs7","t2l","i6qt9","k6","yvql2","tz6","q2","z2l","my94","f7v"};
		DBApp.createTable("dp6", cols1);
		String [][] records_dp6 = new String[91][cols1.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_dp6[i][j] = genRandString();
			}
			DBApp.insert("dp6", records_dp6[i]);
			String tableTrace1 = DBApp.getLastTrace("dp6");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_dp6[i])+")", tableTrace1.contains(Arrays.toString(records_dp6[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"dt4","aa4","ae6","g6s08","r9","ye0","qs9","sf","t26","fn"};
		DBApp.createTable("d2p", cols2);
		String [][] records_d2p = new String[55][cols2.length];
		for(int i=0;i<55;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_d2p[i][j] = genRandString();
			}
			DBApp.insert("d2p", records_d2p[i]);
			String tableTrace2 = DBApp.getLastTrace("d2p");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_d2p[i])+")", tableTrace2.contains(Arrays.toString(records_d2p[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test39TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"nr3"};
		DBApp.createTable("yrvtu", cols0);
		String [][] records_yrvtu = new String[78][cols0.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yrvtu[i][j] = genRandString();
			}
			DBApp.insert("yrvtu", records_yrvtu[i]);
			String tableTrace0 = DBApp.getLastTrace("yrvtu");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_yrvtu[i])+")", tableTrace0.contains(Arrays.toString(records_yrvtu[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"ln35","mb","j0l2y","qrb"};
		DBApp.createTable("eioq", cols1);
		String [][] records_eioq = new String[5][cols1.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_eioq[i][j] = genRandString();
			}
			DBApp.insert("eioq", records_eioq[i]);
			String tableTrace1 = DBApp.getLastTrace("eioq");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_eioq[i])+")", tableTrace1.contains(Arrays.toString(records_eioq[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"aa"};
		DBApp.createTable("tkw68", cols2);
		String [][] records_tkw68 = new String[23][cols2.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_tkw68[i][j] = genRandString();
			}
			DBApp.insert("tkw68", records_tkw68[i]);
			String tableTrace2 = DBApp.getLastTrace("tkw68");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_tkw68[i])+")", tableTrace2.contains(Arrays.toString(records_tkw68[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test40TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"c818","ki5","o32","cf7","go5jg","y2","vy6","nw292","u29"};
		DBApp.createTable("e70n0", cols0);
		String [][] records_e70n0 = new String[95][cols0.length];
		for(int i=0;i<95;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_e70n0[i][j] = genRandString();
			}
			DBApp.insert("e70n0", records_e70n0[i]);
			String tableTrace0 = DBApp.getLastTrace("e70n0");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_e70n0[i])+")", tableTrace0.contains(Arrays.toString(records_e70n0[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test41TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"qi7","y20","lz24","k46"};
		DBApp.createTable("j72c4", cols0);
		String [][] records_j72c4 = new String[27][cols0.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_j72c4[i][j] = genRandString();
			}
			DBApp.insert("j72c4", records_j72c4[i]);
			String tableTrace0 = DBApp.getLastTrace("j72c4");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_j72c4[i])+")", tableTrace0.contains(Arrays.toString(records_j72c4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"lz","v826"};
		DBApp.createTable("afg33", cols1);
		String [][] records_afg33 = new String[50][cols1.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_afg33[i][j] = genRandString();
			}
			DBApp.insert("afg33", records_afg33[i]);
			String tableTrace1 = DBApp.getLastTrace("afg33");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_afg33[i])+")", tableTrace1.contains(Arrays.toString(records_afg33[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"f09g","q2","t5","lagi","j17","v0x4s","g1b","q1y"};
		DBApp.createTable("hsq", cols2);
		String [][] records_hsq = new String[89][cols2.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_hsq[i][j] = genRandString();
			}
			DBApp.insert("hsq", records_hsq[i]);
			String tableTrace2 = DBApp.getLastTrace("hsq");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_hsq[i])+")", tableTrace2.contains(Arrays.toString(records_hsq[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"sxl","a9uu","l37t","sku","c4w2z","ucl","q74","ww9","g75yv"};
		DBApp.createTable("j1", cols3);
		String [][] records_j1 = new String[69][cols3.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_j1[i][j] = genRandString();
			}
			DBApp.insert("j1", records_j1[i]);
			String tableTrace3 = DBApp.getLastTrace("j1");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_j1[i])+")", tableTrace3.contains(Arrays.toString(records_j1[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test42TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"cm","kk7","k0l6"};
		DBApp.createTable("douw", cols0);
		String [][] records_douw = new String[34][cols0.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_douw[i][j] = genRandString();
			}
			DBApp.insert("douw", records_douw[i]);
			String tableTrace0 = DBApp.getLastTrace("douw");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_douw[i])+")", tableTrace0.contains(Arrays.toString(records_douw[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"opm","u5"};
		DBApp.createTable("b1967", cols1);
		String [][] records_b1967 = new String[80][cols1.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_b1967[i][j] = genRandString();
			}
			DBApp.insert("b1967", records_b1967[i]);
			String tableTrace1 = DBApp.getLastTrace("b1967");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_b1967[i])+")", tableTrace1.contains(Arrays.toString(records_b1967[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"hu","fx2","z56t","muk","fi13","a9","t5c11","jzkj"};
		DBApp.createTable("bdx", cols2);
		String [][] records_bdx = new String[13][cols2.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_bdx[i][j] = genRandString();
			}
			DBApp.insert("bdx", records_bdx[i]);
			String tableTrace2 = DBApp.getLastTrace("bdx");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_bdx[i])+")", tableTrace2.contains(Arrays.toString(records_bdx[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test43TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"l91rk","qr27"};
		DBApp.createTable("io46", cols0);
		String [][] records_io46 = new String[91][cols0.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_io46[i][j] = genRandString();
			}
			DBApp.insert("io46", records_io46[i]);
			String tableTrace0 = DBApp.getLastTrace("io46");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_io46[i])+")", tableTrace0.contains(Arrays.toString(records_io46[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"n24"};
		DBApp.createTable("e5a", cols1);
		String [][] records_e5a = new String[29][cols1.length];
		for(int i=0;i<29;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_e5a[i][j] = genRandString();
			}
			DBApp.insert("e5a", records_e5a[i]);
			String tableTrace1 = DBApp.getLastTrace("e5a");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_e5a[i])+")", tableTrace1.contains(Arrays.toString(records_e5a[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test44TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"gg8","sn","r97f","l8u","mn","tu","af6","x8w","x5v7"};
		DBApp.createTable("nm9mv", cols0);
		String [][] records_nm9mv = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_nm9mv[i][j] = genRandString();
			}
			DBApp.insert("nm9mv", records_nm9mv[i]);
			String tableTrace0 = DBApp.getLastTrace("nm9mv");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_nm9mv[i])+")", tableTrace0.contains(Arrays.toString(records_nm9mv[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"nvo","bk","j6o","d9t7","is"};
		DBApp.createTable("n7", cols1);
		String [][] records_n7 = new String[76][cols1.length];
		for(int i=0;i<76;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_n7[i][j] = genRandString();
			}
			DBApp.insert("n7", records_n7[i]);
			String tableTrace1 = DBApp.getLastTrace("n7");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_n7[i])+")", tableTrace1.contains(Arrays.toString(records_n7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"niq6","h63z","m0j","g14"};
		DBApp.createTable("l61", cols2);
		String [][] records_l61 = new String[51][cols2.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_l61[i][j] = genRandString();
			}
			DBApp.insert("l61", records_l61[i]);
			String tableTrace2 = DBApp.getLastTrace("l61");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_l61[i])+")", tableTrace2.contains(Arrays.toString(records_l61[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test45TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"gyh","c0948","a5","qx32","rx","f22","at8"};
		DBApp.createTable("azi", cols0);
		String [][] records_azi = new String[81][cols0.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_azi[i][j] = genRandString();
			}
			DBApp.insert("azi", records_azi[i]);
			String tableTrace0 = DBApp.getLastTrace("azi");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_azi[i])+")", tableTrace0.contains(Arrays.toString(records_azi[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test46TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"ti6wv","pw6","s5","qex","db7i","ti","ucw","y6x","br8","p7"};
		DBApp.createTable("e2p56", cols0);
		String [][] records_e2p56 = new String[43][cols0.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_e2p56[i][j] = genRandString();
			}
			DBApp.insert("e2p56", records_e2p56[i]);
			String tableTrace0 = DBApp.getLastTrace("e2p56");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_e2p56[i])+")", tableTrace0.contains(Arrays.toString(records_e2p56[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"zdo","d94","yz3","b99","agq38","f54","atj5l","qv"};
		DBApp.createTable("kgn2o", cols1);
		String [][] records_kgn2o = new String[17][cols1.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_kgn2o[i][j] = genRandString();
			}
			DBApp.insert("kgn2o", records_kgn2o[i]);
			String tableTrace1 = DBApp.getLastTrace("kgn2o");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_kgn2o[i])+")", tableTrace1.contains(Arrays.toString(records_kgn2o[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"od40","q5"};
		DBApp.createTable("u3y", cols2);
		String [][] records_u3y = new String[98][cols2.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_u3y[i][j] = genRandString();
			}
			DBApp.insert("u3y", records_u3y[i]);
			String tableTrace2 = DBApp.getLastTrace("u3y");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_u3y[i])+")", tableTrace2.contains(Arrays.toString(records_u3y[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"s8y","qz8jl","s7e","vim5w","j3","a2ie","zl055","i75ex","vc824","y1"};
		DBApp.createTable("ty", cols3);
		String [][] records_ty = new String[55][cols3.length];
		for(int i=0;i<55;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ty[i][j] = genRandString();
			}
			DBApp.insert("ty", records_ty[i]);
			String tableTrace3 = DBApp.getLastTrace("ty");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ty[i])+")", tableTrace3.contains(Arrays.toString(records_ty[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"r74","d3ug","at63","wr05","z13","a2"};
		DBApp.createTable("j8", cols4);
		String [][] records_j8 = new String[77][cols4.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_j8[i][j] = genRandString();
			}
			DBApp.insert("j8", records_j8[i]);
			String tableTrace4 = DBApp.getLastTrace("j8");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_j8[i])+")", tableTrace4.contains(Arrays.toString(records_j8[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test47TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"ii97","w8jr","i7czf","n3","a7c","ys9","ed","yd","lkq","a7kg1"};
		DBApp.createTable("vf26", cols0);
		String [][] records_vf26 = new String[76][cols0.length];
		for(int i=0;i<76;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vf26[i][j] = genRandString();
			}
			DBApp.insert("vf26", records_vf26[i]);
			String tableTrace0 = DBApp.getLastTrace("vf26");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_vf26[i])+")", tableTrace0.contains(Arrays.toString(records_vf26[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"c6","kv0","kk3","q1q","qu2","zd913","fb1qr","a2o2"};
		DBApp.createTable("lmw", cols1);
		String [][] records_lmw = new String[53][cols1.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_lmw[i][j] = genRandString();
			}
			DBApp.insert("lmw", records_lmw[i]);
			String tableTrace1 = DBApp.getLastTrace("lmw");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_lmw[i])+")", tableTrace1.contains(Arrays.toString(records_lmw[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"ln4o"};
		DBApp.createTable("ry3", cols2);
		String [][] records_ry3 = new String[100][cols2.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ry3[i][j] = genRandString();
			}
			DBApp.insert("ry3", records_ry3[i]);
			String tableTrace2 = DBApp.getLastTrace("ry3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ry3[i])+")", tableTrace2.contains(Arrays.toString(records_ry3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"g1i","wvv","f95","bdk"};
		DBApp.createTable("x8o", cols3);
		String [][] records_x8o = new String[13][cols3.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_x8o[i][j] = genRandString();
			}
			DBApp.insert("x8o", records_x8o[i]);
			String tableTrace3 = DBApp.getLastTrace("x8o");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_x8o[i])+")", tableTrace3.contains(Arrays.toString(records_x8o[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"h7u"};
		DBApp.createTable("d06", cols4);
		String [][] records_d06 = new String[86][cols4.length];
		for(int i=0;i<86;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_d06[i][j] = genRandString();
			}
			DBApp.insert("d06", records_d06[i]);
			String tableTrace4 = DBApp.getLastTrace("d06");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_d06[i])+")", tableTrace4.contains(Arrays.toString(records_d06[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test48TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"i1"};
		DBApp.createTable("efn", cols0);
		String [][] records_efn = new String[91][cols0.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_efn[i][j] = genRandString();
			}
			DBApp.insert("efn", records_efn[i]);
			String tableTrace0 = DBApp.getLastTrace("efn");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_efn[i])+")", tableTrace0.contains(Arrays.toString(records_efn[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test49TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"n1","f3c"};
		DBApp.createTable("v069", cols0);
		String [][] records_v069 = new String[65][cols0.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_v069[i][j] = genRandString();
			}
			DBApp.insert("v069", records_v069[i]);
			String tableTrace0 = DBApp.getLastTrace("v069");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_v069[i])+")", tableTrace0.contains(Arrays.toString(records_v069[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test50TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"d05","h30","du","i8x"};
		DBApp.createTable("om6", cols0);
		String [][] records_om6 = new String[89][cols0.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_om6[i][j] = genRandString();
			}
			DBApp.insert("om6", records_om6[i]);
			String tableTrace0 = DBApp.getLastTrace("om6");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_om6[i])+")", tableTrace0.contains(Arrays.toString(records_om6[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"mc8xt","d36","pw6","ytj","cc"};
		DBApp.createTable("uig", cols1);
		String [][] records_uig = new String[67][cols1.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_uig[i][j] = genRandString();
			}
			DBApp.insert("uig", records_uig[i]);
			String tableTrace1 = DBApp.getLastTrace("uig");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_uig[i])+")", tableTrace1.contains(Arrays.toString(records_uig[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test51TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"z7","dq3","t0","tm","nrl","cz3","b84","p9022","k21"};
		DBApp.createTable("p10", cols0);
		String [][] records_p10 = new String[27][cols0.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p10[i][j] = genRandString();
			}
			DBApp.insert("p10", records_p10[i]);
			String tableTrace0 = DBApp.getLastTrace("p10");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_p10[i])+")", tableTrace0.contains(Arrays.toString(records_p10[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"sgk52","k9oi","m31","xg00","v7","s8"};
		DBApp.createTable("v1m", cols1);
		String [][] records_v1m = new String[34][cols1.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_v1m[i][j] = genRandString();
			}
			DBApp.insert("v1m", records_v1m[i]);
			String tableTrace1 = DBApp.getLastTrace("v1m");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_v1m[i])+")", tableTrace1.contains(Arrays.toString(records_v1m[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"s974j","b13","uks"};
		DBApp.createTable("pa99", cols2);
		String [][] records_pa99 = new String[77][cols2.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_pa99[i][j] = genRandString();
			}
			DBApp.insert("pa99", records_pa99[i]);
			String tableTrace2 = DBApp.getLastTrace("pa99");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_pa99[i])+")", tableTrace2.contains(Arrays.toString(records_pa99[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test52TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"sd7","dsttk","y01","biu","r3y","s3c","v761","d72g4","szd","w2b2d"};
		DBApp.createTable("yg2", cols0);
		String [][] records_yg2 = new String[70][cols0.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yg2[i][j] = genRandString();
			}
			DBApp.insert("yg2", records_yg2[i]);
			String tableTrace0 = DBApp.getLastTrace("yg2");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_yg2[i])+")", tableTrace0.contains(Arrays.toString(records_yg2[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test53TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"e53","pn","m47","t0ci","qnf","wz","up6","jl5l","q6c"};
		DBApp.createTable("n4j", cols0);
		String [][] records_n4j = new String[74][cols0.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_n4j[i][j] = genRandString();
			}
			DBApp.insert("n4j", records_n4j[i]);
			String tableTrace0 = DBApp.getLastTrace("n4j");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_n4j[i])+")", tableTrace0.contains(Arrays.toString(records_n4j[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"ze","q71","eqea","frk2","jg8gm","n1z","r69"};
		DBApp.createTable("k5w5", cols1);
		String [][] records_k5w5 = new String[56][cols1.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_k5w5[i][j] = genRandString();
			}
			DBApp.insert("k5w5", records_k5w5[i]);
			String tableTrace1 = DBApp.getLastTrace("k5w5");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_k5w5[i])+")", tableTrace1.contains(Arrays.toString(records_k5w5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"z89p","p0","d5c","y65","n2o","f5q","kbc","owt","qpd"};
		DBApp.createTable("y6", cols2);
		String [][] records_y6 = new String[99][cols2.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_y6[i][j] = genRandString();
			}
			DBApp.insert("y6", records_y6[i]);
			String tableTrace2 = DBApp.getLastTrace("y6");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_y6[i])+")", tableTrace2.contains(Arrays.toString(records_y6[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test54TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"pm4w","hbot9","wgb2","u8re","k28","qi6d","m4","t304y","l3u","b98"};
		DBApp.createTable("y0kh", cols0);
		String [][] records_y0kh = new String[3][cols0.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_y0kh[i][j] = genRandString();
			}
			DBApp.insert("y0kh", records_y0kh[i]);
			String tableTrace0 = DBApp.getLastTrace("y0kh");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_y0kh[i])+")", tableTrace0.contains(Arrays.toString(records_y0kh[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"n17y","t4","x4hf0","iksj1","c2718","wqx"};
		DBApp.createTable("lcu0", cols1);
		String [][] records_lcu0 = new String[21][cols1.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_lcu0[i][j] = genRandString();
			}
			DBApp.insert("lcu0", records_lcu0[i]);
			String tableTrace1 = DBApp.getLastTrace("lcu0");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_lcu0[i])+")", tableTrace1.contains(Arrays.toString(records_lcu0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"p2","t2","p2h"};
		DBApp.createTable("n8", cols2);
		String [][] records_n8 = new String[61][cols2.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_n8[i][j] = genRandString();
			}
			DBApp.insert("n8", records_n8[i]);
			String tableTrace2 = DBApp.getLastTrace("n8");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_n8[i])+")", tableTrace2.contains(Arrays.toString(records_n8[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test55TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"t91","s53e","a8l","z5742"};
		DBApp.createTable("ow9mg", cols0);
		String [][] records_ow9mg = new String[71][cols0.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ow9mg[i][j] = genRandString();
			}
			DBApp.insert("ow9mg", records_ow9mg[i]);
			String tableTrace0 = DBApp.getLastTrace("ow9mg");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ow9mg[i])+")", tableTrace0.contains(Arrays.toString(records_ow9mg[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"sm396","q3","pl","qm","caxe4","nd6"};
		DBApp.createTable("c0", cols1);
		String [][] records_c0 = new String[20][cols1.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_c0[i][j] = genRandString();
			}
			DBApp.insert("c0", records_c0[i]);
			String tableTrace1 = DBApp.getLastTrace("c0");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_c0[i])+")", tableTrace1.contains(Arrays.toString(records_c0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"kx9","s8","bk4","xt7","sv3h"};
		DBApp.createTable("y78a8", cols2);
		String [][] records_y78a8 = new String[23][cols2.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_y78a8[i][j] = genRandString();
			}
			DBApp.insert("y78a8", records_y78a8[i]);
			String tableTrace2 = DBApp.getLastTrace("y78a8");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_y78a8[i])+")", tableTrace2.contains(Arrays.toString(records_y78a8[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"h3q","l2f","w8","pv","z6d","rl4ze","k81"};
		DBApp.createTable("m7qa3", cols3);
		String [][] records_m7qa3 = new String[27][cols3.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_m7qa3[i][j] = genRandString();
			}
			DBApp.insert("m7qa3", records_m7qa3[i]);
			String tableTrace3 = DBApp.getLastTrace("m7qa3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_m7qa3[i])+")", tableTrace3.contains(Arrays.toString(records_m7qa3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"veq","u54"};
		DBApp.createTable("ffy", cols4);
		String [][] records_ffy = new String[23][cols4.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_ffy[i][j] = genRandString();
			}
			DBApp.insert("ffy", records_ffy[i]);
			String tableTrace4 = DBApp.getLastTrace("ffy");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ffy[i])+")", tableTrace4.contains(Arrays.toString(records_ffy[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test56TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"y6j5","a92","yg","x1","k1592","je"};
		DBApp.createTable("t8a96", cols0);
		String [][] records_t8a96 = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t8a96[i][j] = genRandString();
			}
			DBApp.insert("t8a96", records_t8a96[i]);
			String tableTrace0 = DBApp.getLastTrace("t8a96");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_t8a96[i])+")", tableTrace0.contains(Arrays.toString(records_t8a96[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"lecb2","no768","xj","m36","ezl","i5","o43","g8b","kf1","x3"};
		DBApp.createTable("k3s2j", cols1);
		String [][] records_k3s2j = new String[46][cols1.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_k3s2j[i][j] = genRandString();
			}
			DBApp.insert("k3s2j", records_k3s2j[i]);
			String tableTrace1 = DBApp.getLastTrace("k3s2j");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_k3s2j[i])+")", tableTrace1.contains(Arrays.toString(records_k3s2j[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"t34p","e666","xejrl","gl","zt49","ktmp","c0b","tsvz"};
		DBApp.createTable("w3z", cols2);
		String [][] records_w3z = new String[89][cols2.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_w3z[i][j] = genRandString();
			}
			DBApp.insert("w3z", records_w3z[i]);
			String tableTrace2 = DBApp.getLastTrace("w3z");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_w3z[i])+")", tableTrace2.contains(Arrays.toString(records_w3z[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"b74","o29ju","ai9ju","px9","non7","q3"};
		DBApp.createTable("u02", cols3);
		String [][] records_u02 = new String[19][cols3.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_u02[i][j] = genRandString();
			}
			DBApp.insert("u02", records_u02[i]);
			String tableTrace3 = DBApp.getLastTrace("u02");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_u02[i])+")", tableTrace3.contains(Arrays.toString(records_u02[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"zsn9","w00i9","t9","d0","j1","i37","fd","h0878","phf"};
		DBApp.createTable("p1991", cols4);
		String [][] records_p1991 = new String[8][cols4.length];
		for(int i=0;i<8;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_p1991[i][j] = genRandString();
			}
			DBApp.insert("p1991", records_p1991[i]);
			String tableTrace4 = DBApp.getLastTrace("p1991");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_p1991[i])+")", tableTrace4.contains(Arrays.toString(records_p1991[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test57TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"g3308"};
		DBApp.createTable("x0", cols0);
		String [][] records_x0 = new String[73][cols0.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x0[i][j] = genRandString();
			}
			DBApp.insert("x0", records_x0[i]);
			String tableTrace0 = DBApp.getLastTrace("x0");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_x0[i])+")", tableTrace0.contains(Arrays.toString(records_x0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"yni","m8m","iox","m4","awc","b9o","bmt","q61d","ltu96"};
		DBApp.createTable("f7uq", cols1);
		String [][] records_f7uq = new String[94][cols1.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_f7uq[i][j] = genRandString();
			}
			DBApp.insert("f7uq", records_f7uq[i]);
			String tableTrace1 = DBApp.getLastTrace("f7uq");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_f7uq[i])+")", tableTrace1.contains(Arrays.toString(records_f7uq[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"m6","n9l","rs96","n01"};
		DBApp.createTable("l76", cols2);
		String [][] records_l76 = new String[37][cols2.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_l76[i][j] = genRandString();
			}
			DBApp.insert("l76", records_l76[i]);
			String tableTrace2 = DBApp.getLastTrace("l76");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_l76[i])+")", tableTrace2.contains(Arrays.toString(records_l76[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test58TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"rm9j","k487n","usr"};
		DBApp.createTable("pp", cols0);
		String [][] records_pp = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_pp[i][j] = genRandString();
			}
			DBApp.insert("pp", records_pp[i]);
			String tableTrace0 = DBApp.getLastTrace("pp");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_pp[i])+")", tableTrace0.contains(Arrays.toString(records_pp[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"ffq20"};
		DBApp.createTable("h9o7", cols1);
		String [][] records_h9o7 = new String[90][cols1.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_h9o7[i][j] = genRandString();
			}
			DBApp.insert("h9o7", records_h9o7[i]);
			String tableTrace1 = DBApp.getLastTrace("h9o7");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_h9o7[i])+")", tableTrace1.contains(Arrays.toString(records_h9o7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"z15","d93","r86","d697k","gb2t2","q4d","q6"};
		DBApp.createTable("ng", cols2);
		String [][] records_ng = new String[4][cols2.length];
		for(int i=0;i<4;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ng[i][j] = genRandString();
			}
			DBApp.insert("ng", records_ng[i]);
			String tableTrace2 = DBApp.getLastTrace("ng");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ng[i])+")", tableTrace2.contains(Arrays.toString(records_ng[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"g76","g4","nqd","tuzyo"};
		DBApp.createTable("n43sq", cols3);
		String [][] records_n43sq = new String[46][cols3.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_n43sq[i][j] = genRandString();
			}
			DBApp.insert("n43sq", records_n43sq[i]);
			String tableTrace3 = DBApp.getLastTrace("n43sq");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_n43sq[i])+")", tableTrace3.contains(Arrays.toString(records_n43sq[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"bg1"};
		DBApp.createTable("e4l", cols4);
		String [][] records_e4l = new String[61][cols4.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_e4l[i][j] = genRandString();
			}
			DBApp.insert("e4l", records_e4l[i]);
			String tableTrace4 = DBApp.getLastTrace("e4l");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_e4l[i])+")", tableTrace4.contains(Arrays.toString(records_e4l[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test59TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a7927","r55k","d9","np9","agj","za7ou","xn8","zn","fxa0","w83"};
		DBApp.createTable("m23vn", cols0);
		String [][] records_m23vn = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_m23vn[i][j] = genRandString();
			}
			DBApp.insert("m23vn", records_m23vn[i]);
			String tableTrace0 = DBApp.getLastTrace("m23vn");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_m23vn[i])+")", tableTrace0.contains(Arrays.toString(records_m23vn[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test60TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"j21q","wie","zm41z"};
		DBApp.createTable("k9z", cols0);
		String [][] records_k9z = new String[76][cols0.length];
		for(int i=0;i<76;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k9z[i][j] = genRandString();
			}
			DBApp.insert("k9z", records_k9z[i]);
			String tableTrace0 = DBApp.getLastTrace("k9z");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_k9z[i])+")", tableTrace0.contains(Arrays.toString(records_k9z[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"sa4","g301v","z3k"};
		DBApp.createTable("waw31", cols1);
		String [][] records_waw31 = new String[78][cols1.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_waw31[i][j] = genRandString();
			}
			DBApp.insert("waw31", records_waw31[i]);
			String tableTrace1 = DBApp.getLastTrace("waw31");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_waw31[i])+")", tableTrace1.contains(Arrays.toString(records_waw31[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"js","r81c","d30db","j315s","bhv","ymi"};
		DBApp.createTable("zg5", cols2);
		String [][] records_zg5 = new String[7][cols2.length];
		for(int i=0;i<7;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_zg5[i][j] = genRandString();
			}
			DBApp.insert("zg5", records_zg5[i]);
			String tableTrace2 = DBApp.getLastTrace("zg5");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_zg5[i])+")", tableTrace2.contains(Arrays.toString(records_zg5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"o23tx","kpo","g813e"};
		DBApp.createTable("gc", cols3);
		String [][] records_gc = new String[98][cols3.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_gc[i][j] = genRandString();
			}
			DBApp.insert("gc", records_gc[i]);
			String tableTrace3 = DBApp.getLastTrace("gc");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_gc[i])+")", tableTrace3.contains(Arrays.toString(records_gc[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"x8h","d5d","v5s3","ri0v","vgx","g1m","s7td","p74o6"};
		DBApp.createTable("k6i", cols4);
		String [][] records_k6i = new String[39][cols4.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_k6i[i][j] = genRandString();
			}
			DBApp.insert("k6i", records_k6i[i]);
			String tableTrace4 = DBApp.getLastTrace("k6i");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_k6i[i])+")", tableTrace4.contains(Arrays.toString(records_k6i[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test61TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"ss6cj","x1","e94w"};
		DBApp.createTable("el", cols0);
		String [][] records_el = new String[81][cols0.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_el[i][j] = genRandString();
			}
			DBApp.insert("el", records_el[i]);
			String tableTrace0 = DBApp.getLastTrace("el");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_el[i])+")", tableTrace0.contains(Arrays.toString(records_el[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"y73xt","ww","a0","zuo9","pq","cy"};
		DBApp.createTable("e6", cols1);
		String [][] records_e6 = new String[71][cols1.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_e6[i][j] = genRandString();
			}
			DBApp.insert("e6", records_e6[i]);
			String tableTrace1 = DBApp.getLastTrace("e6");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_e6[i])+")", tableTrace1.contains(Arrays.toString(records_e6[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"u0n","y3wu3"};
		DBApp.createTable("ve7", cols2);
		String [][] records_ve7 = new String[77][cols2.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ve7[i][j] = genRandString();
			}
			DBApp.insert("ve7", records_ve7[i]);
			String tableTrace2 = DBApp.getLastTrace("ve7");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ve7[i])+")", tableTrace2.contains(Arrays.toString(records_ve7[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test62TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"xd731","h0d","ab7","whzv","xzz3d","bc6h7","uu","v32","jk3","b00e"};
		DBApp.createTable("vtl", cols0);
		String [][] records_vtl = new String[37][cols0.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vtl[i][j] = genRandString();
			}
			DBApp.insert("vtl", records_vtl[i]);
			String tableTrace0 = DBApp.getLastTrace("vtl");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_vtl[i])+")", tableTrace0.contains(Arrays.toString(records_vtl[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"uu","o049","qw","ohc9","eo","vhfj","cv","g1r","xat","ezx85"};
		DBApp.createTable("xn", cols1);
		String [][] records_xn = new String[54][cols1.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_xn[i][j] = genRandString();
			}
			DBApp.insert("xn", records_xn[i]);
			String tableTrace1 = DBApp.getLastTrace("xn");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_xn[i])+")", tableTrace1.contains(Arrays.toString(records_xn[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"l2z","e1j","w40","fj","b4","k3"};
		DBApp.createTable("e94", cols2);
		String [][] records_e94 = new String[28][cols2.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_e94[i][j] = genRandString();
			}
			DBApp.insert("e94", records_e94[i]);
			String tableTrace2 = DBApp.getLastTrace("e94");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_e94[i])+")", tableTrace2.contains(Arrays.toString(records_e94[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test63TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"yr","znpv"};
		DBApp.createTable("i9", cols0);
		String [][] records_i9 = new String[60][cols0.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i9[i][j] = genRandString();
			}
			DBApp.insert("i9", records_i9[i]);
			String tableTrace0 = DBApp.getLastTrace("i9");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_i9[i])+")", tableTrace0.contains(Arrays.toString(records_i9[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"ni"};
		DBApp.createTable("e0dn", cols1);
		String [][] records_e0dn = new String[32][cols1.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_e0dn[i][j] = genRandString();
			}
			DBApp.insert("e0dn", records_e0dn[i]);
			String tableTrace1 = DBApp.getLastTrace("e0dn");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_e0dn[i])+")", tableTrace1.contains(Arrays.toString(records_e0dn[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"n00","a1s","zk","t2du","gjql","o2t","s24","k0"};
		DBApp.createTable("pk44x", cols2);
		String [][] records_pk44x = new String[88][cols2.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_pk44x[i][j] = genRandString();
			}
			DBApp.insert("pk44x", records_pk44x[i]);
			String tableTrace2 = DBApp.getLastTrace("pk44x");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_pk44x[i])+")", tableTrace2.contains(Arrays.toString(records_pk44x[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"cc"};
		DBApp.createTable("cm7r", cols3);
		String [][] records_cm7r = new String[15][cols3.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_cm7r[i][j] = genRandString();
			}
			DBApp.insert("cm7r", records_cm7r[i]);
			String tableTrace3 = DBApp.getLastTrace("cm7r");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_cm7r[i])+")", tableTrace3.contains(Arrays.toString(records_cm7r[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test64TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"g310r","dlc","g89yh","p91"};
		DBApp.createTable("t8w", cols0);
		String [][] records_t8w = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t8w[i][j] = genRandString();
			}
			DBApp.insert("t8w", records_t8w[i]);
			String tableTrace0 = DBApp.getLastTrace("t8w");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_t8w[i])+")", tableTrace0.contains(Arrays.toString(records_t8w[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"xzd","x8","q420","fv66"};
		DBApp.createTable("g9od", cols1);
		String [][] records_g9od = new String[69][cols1.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_g9od[i][j] = genRandString();
			}
			DBApp.insert("g9od", records_g9od[i]);
			String tableTrace1 = DBApp.getLastTrace("g9od");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_g9od[i])+")", tableTrace1.contains(Arrays.toString(records_g9od[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"p9w","guga","p3"};
		DBApp.createTable("ujm0i", cols2);
		String [][] records_ujm0i = new String[83][cols2.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ujm0i[i][j] = genRandString();
			}
			DBApp.insert("ujm0i", records_ujm0i[i]);
			String tableTrace2 = DBApp.getLastTrace("ujm0i");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ujm0i[i])+")", tableTrace2.contains(Arrays.toString(records_ujm0i[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"guy","g48","qyf0x","u5742","tvvj","uy98q"};
		DBApp.createTable("ol7", cols3);
		String [][] records_ol7 = new String[55][cols3.length];
		for(int i=0;i<55;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ol7[i][j] = genRandString();
			}
			DBApp.insert("ol7", records_ol7[i]);
			String tableTrace3 = DBApp.getLastTrace("ol7");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ol7[i])+")", tableTrace3.contains(Arrays.toString(records_ol7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"eux","o9zf7"};
		DBApp.createTable("mkg", cols4);
		String [][] records_mkg = new String[33][cols4.length];
		for(int i=0;i<33;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_mkg[i][j] = genRandString();
			}
			DBApp.insert("mkg", records_mkg[i]);
			String tableTrace4 = DBApp.getLastTrace("mkg");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_mkg[i])+")", tableTrace4.contains(Arrays.toString(records_mkg[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test65TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"ur","k04","dy976","zw","o7","h7bfn","w23"};
		DBApp.createTable("s2pa", cols0);
		String [][] records_s2pa = new String[17][cols0.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_s2pa[i][j] = genRandString();
			}
			DBApp.insert("s2pa", records_s2pa[i]);
			String tableTrace0 = DBApp.getLastTrace("s2pa");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_s2pa[i])+")", tableTrace0.contains(Arrays.toString(records_s2pa[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"e14","t7xig"};
		DBApp.createTable("g07", cols1);
		String [][] records_g07 = new String[72][cols1.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_g07[i][j] = genRandString();
			}
			DBApp.insert("g07", records_g07[i]);
			String tableTrace1 = DBApp.getLastTrace("g07");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_g07[i])+")", tableTrace1.contains(Arrays.toString(records_g07[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test66TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"oo4","p3t1","w81","wq9td","m8g","pll","w5","rc","oj6","el"};
		DBApp.createTable("ww", cols0);
		String [][] records_ww = new String[100][cols0.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ww[i][j] = genRandString();
			}
			DBApp.insert("ww", records_ww[i]);
			String tableTrace0 = DBApp.getLastTrace("ww");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ww[i])+")", tableTrace0.contains(Arrays.toString(records_ww[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"m62","cs1","l75","ly","dy","h33"};
		DBApp.createTable("am2", cols1);
		String [][] records_am2 = new String[94][cols1.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_am2[i][j] = genRandString();
			}
			DBApp.insert("am2", records_am2[i]);
			String tableTrace1 = DBApp.getLastTrace("am2");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_am2[i])+")", tableTrace1.contains(Arrays.toString(records_am2[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test67TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"wns8","se9w","dagb"};
		DBApp.createTable("f3n5e", cols0);
		String [][] records_f3n5e = new String[83][cols0.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_f3n5e[i][j] = genRandString();
			}
			DBApp.insert("f3n5e", records_f3n5e[i]);
			String tableTrace0 = DBApp.getLastTrace("f3n5e");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_f3n5e[i])+")", tableTrace0.contains(Arrays.toString(records_f3n5e[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"r3h","e0u7","e16","bi2","m0z59","bw5","k70c","oyj","tto"};
		DBApp.createTable("ub", cols1);
		String [][] records_ub = new String[2][cols1.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ub[i][j] = genRandString();
			}
			DBApp.insert("ub", records_ub[i]);
			String tableTrace1 = DBApp.getLastTrace("ub");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ub[i])+")", tableTrace1.contains(Arrays.toString(records_ub[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"kpej"};
		DBApp.createTable("kop", cols2);
		String [][] records_kop = new String[99][cols2.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_kop[i][j] = genRandString();
			}
			DBApp.insert("kop", records_kop[i]);
			String tableTrace2 = DBApp.getLastTrace("kop");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_kop[i])+")", tableTrace2.contains(Arrays.toString(records_kop[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"yk81"};
		DBApp.createTable("c83", cols3);
		String [][] records_c83 = new String[17][cols3.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_c83[i][j] = genRandString();
			}
			DBApp.insert("c83", records_c83[i]);
			String tableTrace3 = DBApp.getLastTrace("c83");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_c83[i])+")", tableTrace3.contains(Arrays.toString(records_c83[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"kd","ly00","gq4"};
		DBApp.createTable("f1e", cols4);
		String [][] records_f1e = new String[1][cols4.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_f1e[i][j] = genRandString();
			}
			DBApp.insert("f1e", records_f1e[i]);
			String tableTrace4 = DBApp.getLastTrace("f1e");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_f1e[i])+")", tableTrace4.contains(Arrays.toString(records_f1e[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test68TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"l95","wc8r"};
		DBApp.createTable("r4q", cols0);
		String [][] records_r4q = new String[71][cols0.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r4q[i][j] = genRandString();
			}
			DBApp.insert("r4q", records_r4q[i]);
			String tableTrace0 = DBApp.getLastTrace("r4q");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_r4q[i])+")", tableTrace0.contains(Arrays.toString(records_r4q[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"p72","b7bck","mk2"};
		DBApp.createTable("q32", cols1);
		String [][] records_q32 = new String[78][cols1.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_q32[i][j] = genRandString();
			}
			DBApp.insert("q32", records_q32[i]);
			String tableTrace1 = DBApp.getLastTrace("q32");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_q32[i])+")", tableTrace1.contains(Arrays.toString(records_q32[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"k4","d5sbg","w350","sf8","o89","q7lq","ytq2","wm2f","pj9"};
		DBApp.createTable("sqb3w", cols2);
		String [][] records_sqb3w = new String[73][cols2.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_sqb3w[i][j] = genRandString();
			}
			DBApp.insert("sqb3w", records_sqb3w[i]);
			String tableTrace2 = DBApp.getLastTrace("sqb3w");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_sqb3w[i])+")", tableTrace2.contains(Arrays.toString(records_sqb3w[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"qx6","a8j07","d3k7","bn2tv","vs76x","ny9","rc","d8","x8","j4kn7"};
		DBApp.createTable("wq", cols3);
		String [][] records_wq = new String[22][cols3.length];
		for(int i=0;i<22;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_wq[i][j] = genRandString();
			}
			DBApp.insert("wq", records_wq[i]);
			String tableTrace3 = DBApp.getLastTrace("wq");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_wq[i])+")", tableTrace3.contains(Arrays.toString(records_wq[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"os","c40","d10v","c2","x4rf8"};
		DBApp.createTable("f6i", cols4);
		String [][] records_f6i = new String[21][cols4.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_f6i[i][j] = genRandString();
			}
			DBApp.insert("f6i", records_f6i[i]);
			String tableTrace4 = DBApp.getLastTrace("f6i");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_f6i[i])+")", tableTrace4.contains(Arrays.toString(records_f6i[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test69TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"zecy2","so71i","vlg","mf"};
		DBApp.createTable("b32", cols0);
		String [][] records_b32 = new String[62][cols0.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_b32[i][j] = genRandString();
			}
			DBApp.insert("b32", records_b32[i]);
			String tableTrace0 = DBApp.getLastTrace("b32");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_b32[i])+")", tableTrace0.contains(Arrays.toString(records_b32[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"x8h70","vp1","h2v","t6s","sq96","i75","ip1","kt5","un"};
		DBApp.createTable("ln7", cols1);
		String [][] records_ln7 = new String[25][cols1.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ln7[i][j] = genRandString();
			}
			DBApp.insert("ln7", records_ln7[i]);
			String tableTrace1 = DBApp.getLastTrace("ln7");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ln7[i])+")", tableTrace1.contains(Arrays.toString(records_ln7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"b93","qz","c1"};
		DBApp.createTable("kpe82", cols2);
		String [][] records_kpe82 = new String[89][cols2.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_kpe82[i][j] = genRandString();
			}
			DBApp.insert("kpe82", records_kpe82[i]);
			String tableTrace2 = DBApp.getLastTrace("kpe82");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_kpe82[i])+")", tableTrace2.contains(Arrays.toString(records_kpe82[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test70TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"fjm","g224r","f7p","t3","cy"};
		DBApp.createTable("mmk5a", cols0);
		String [][] records_mmk5a = new String[80][cols0.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_mmk5a[i][j] = genRandString();
			}
			DBApp.insert("mmk5a", records_mmk5a[i]);
			String tableTrace0 = DBApp.getLastTrace("mmk5a");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_mmk5a[i])+")", tableTrace0.contains(Arrays.toString(records_mmk5a[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"o1"};
		DBApp.createTable("i2at0", cols1);
		String [][] records_i2at0 = new String[3][cols1.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_i2at0[i][j] = genRandString();
			}
			DBApp.insert("i2at0", records_i2at0[i]);
			String tableTrace1 = DBApp.getLastTrace("i2at0");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_i2at0[i])+")", tableTrace1.contains(Arrays.toString(records_i2at0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"m8","r14","hx","km33j","g3","lai2"};
		DBApp.createTable("od0d", cols2);
		String [][] records_od0d = new String[96][cols2.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_od0d[i][j] = genRandString();
			}
			DBApp.insert("od0d", records_od0d[i]);
			String tableTrace2 = DBApp.getLastTrace("od0d");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_od0d[i])+")", tableTrace2.contains(Arrays.toString(records_od0d[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test71TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"ld","u0i","ydvpi","on","t63","r03xu","vj9l","ef"};
		DBApp.createTable("xj4t", cols0);
		String [][] records_xj4t = new String[87][cols0.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_xj4t[i][j] = genRandString();
			}
			DBApp.insert("xj4t", records_xj4t[i]);
			String tableTrace0 = DBApp.getLastTrace("xj4t");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_xj4t[i])+")", tableTrace0.contains(Arrays.toString(records_xj4t[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"hna","ebo","sms","r3"};
		DBApp.createTable("b14c4", cols1);
		String [][] records_b14c4 = new String[93][cols1.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_b14c4[i][j] = genRandString();
			}
			DBApp.insert("b14c4", records_b14c4[i]);
			String tableTrace1 = DBApp.getLastTrace("b14c4");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_b14c4[i])+")", tableTrace1.contains(Arrays.toString(records_b14c4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"z00a1","sdw9","j0d","v84","q5"};
		DBApp.createTable("qj2", cols2);
		String [][] records_qj2 = new String[58][cols2.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_qj2[i][j] = genRandString();
			}
			DBApp.insert("qj2", records_qj2[i]);
			String tableTrace2 = DBApp.getLastTrace("qj2");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_qj2[i])+")", tableTrace2.contains(Arrays.toString(records_qj2[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"rz2u","y0","ck2"};
		DBApp.createTable("jj", cols3);
		String [][] records_jj = new String[53][cols3.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_jj[i][j] = genRandString();
			}
			DBApp.insert("jj", records_jj[i]);
			String tableTrace3 = DBApp.getLastTrace("jj");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_jj[i])+")", tableTrace3.contains(Arrays.toString(records_jj[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test72TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"t0k42","rd733","uh33","y1","w6","afp2"};
		DBApp.createTable("lk", cols0);
		String [][] records_lk = new String[75][cols0.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_lk[i][j] = genRandString();
			}
			DBApp.insert("lk", records_lk[i]);
			String tableTrace0 = DBApp.getLastTrace("lk");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_lk[i])+")", tableTrace0.contains(Arrays.toString(records_lk[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"j64v","b0","mhu","a9wj9","q3","qed","gs87"};
		DBApp.createTable("m43", cols1);
		String [][] records_m43 = new String[16][cols1.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_m43[i][j] = genRandString();
			}
			DBApp.insert("m43", records_m43[i]);
			String tableTrace1 = DBApp.getLastTrace("m43");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_m43[i])+")", tableTrace1.contains(Arrays.toString(records_m43[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"ly8"};
		DBApp.createTable("mlj", cols2);
		String [][] records_mlj = new String[85][cols2.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_mlj[i][j] = genRandString();
			}
			DBApp.insert("mlj", records_mlj[i]);
			String tableTrace2 = DBApp.getLastTrace("mlj");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_mlj[i])+")", tableTrace2.contains(Arrays.toString(records_mlj[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"sc98","cg8"};
		DBApp.createTable("j92", cols3);
		String [][] records_j92 = new String[63][cols3.length];
		for(int i=0;i<63;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_j92[i][j] = genRandString();
			}
			DBApp.insert("j92", records_j92[i]);
			String tableTrace3 = DBApp.getLastTrace("j92");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_j92[i])+")", tableTrace3.contains(Arrays.toString(records_j92[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test73TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"l74by","pk7","kf0"};
		DBApp.createTable("q48", cols0);
		String [][] records_q48 = new String[38][cols0.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_q48[i][j] = genRandString();
			}
			DBApp.insert("q48", records_q48[i]);
			String tableTrace0 = DBApp.getLastTrace("q48");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_q48[i])+")", tableTrace0.contains(Arrays.toString(records_q48[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test74TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"t20","qpt","gfh","q2xfv","fa0t"};
		DBApp.createTable("ao5ch", cols0);
		String [][] records_ao5ch = new String[72][cols0.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ao5ch[i][j] = genRandString();
			}
			DBApp.insert("ao5ch", records_ao5ch[i]);
			String tableTrace0 = DBApp.getLastTrace("ao5ch");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ao5ch[i])+")", tableTrace0.contains(Arrays.toString(records_ao5ch[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"k0","i8"};
		DBApp.createTable("svtc", cols1);
		String [][] records_svtc = new String[84][cols1.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_svtc[i][j] = genRandString();
			}
			DBApp.insert("svtc", records_svtc[i]);
			String tableTrace1 = DBApp.getLastTrace("svtc");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_svtc[i])+")", tableTrace1.contains(Arrays.toString(records_svtc[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"vtf","ppvbp","yc","ew01","y037"};
		DBApp.createTable("v2d", cols2);
		String [][] records_v2d = new String[27][cols2.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_v2d[i][j] = genRandString();
			}
			DBApp.insert("v2d", records_v2d[i]);
			String tableTrace2 = DBApp.getLastTrace("v2d");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_v2d[i])+")", tableTrace2.contains(Arrays.toString(records_v2d[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test75TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"op6","a3j","zy","c1r","b234w","b48","ri4","bjx32"};
		DBApp.createTable("z9z", cols0);
		String [][] records_z9z = new String[93][cols0.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_z9z[i][j] = genRandString();
			}
			DBApp.insert("z9z", records_z9z[i]);
			String tableTrace0 = DBApp.getLastTrace("z9z");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_z9z[i])+")", tableTrace0.contains(Arrays.toString(records_z9z[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"nt5m","b5","y7","pw0","wz1","l4"};
		DBApp.createTable("mqs", cols1);
		String [][] records_mqs = new String[47][cols1.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_mqs[i][j] = genRandString();
			}
			DBApp.insert("mqs", records_mqs[i]);
			String tableTrace1 = DBApp.getLastTrace("mqs");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_mqs[i])+")", tableTrace1.contains(Arrays.toString(records_mqs[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test76TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"w0ob6"};
		DBApp.createTable("xwm", cols0);
		String [][] records_xwm = new String[51][cols0.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_xwm[i][j] = genRandString();
			}
			DBApp.insert("xwm", records_xwm[i]);
			String tableTrace0 = DBApp.getLastTrace("xwm");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_xwm[i])+")", tableTrace0.contains(Arrays.toString(records_xwm[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"xnb","b8n","yws7","ax11"};
		DBApp.createTable("rg78", cols1);
		String [][] records_rg78 = new String[15][cols1.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_rg78[i][j] = genRandString();
			}
			DBApp.insert("rg78", records_rg78[i]);
			String tableTrace1 = DBApp.getLastTrace("rg78");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_rg78[i])+")", tableTrace1.contains(Arrays.toString(records_rg78[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"gwb"};
		DBApp.createTable("g6", cols2);
		String [][] records_g6 = new String[35][cols2.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_g6[i][j] = genRandString();
			}
			DBApp.insert("g6", records_g6[i]);
			String tableTrace2 = DBApp.getLastTrace("g6");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_g6[i])+")", tableTrace2.contains(Arrays.toString(records_g6[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"y7w","up","que9n","hnh","n2v","ixsb","co3c","z2r4","o1p","q6"};
		DBApp.createTable("f19jc", cols3);
		String [][] records_f19jc = new String[51][cols3.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_f19jc[i][j] = genRandString();
			}
			DBApp.insert("f19jc", records_f19jc[i]);
			String tableTrace3 = DBApp.getLastTrace("f19jc");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_f19jc[i])+")", tableTrace3.contains(Arrays.toString(records_f19jc[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test77TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"w2i","t69","j6","ax9","vh","h00","yeg","yv6"};
		DBApp.createTable("x3", cols0);
		String [][] records_x3 = new String[47][cols0.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x3[i][j] = genRandString();
			}
			DBApp.insert("x3", records_x3[i]);
			String tableTrace0 = DBApp.getLastTrace("x3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_x3[i])+")", tableTrace0.contains(Arrays.toString(records_x3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"f643","k1b56","sf5b","yt","u2","m7c","p3","t3q2","o60b"};
		DBApp.createTable("bvy", cols1);
		String [][] records_bvy = new String[45][cols1.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_bvy[i][j] = genRandString();
			}
			DBApp.insert("bvy", records_bvy[i]);
			String tableTrace1 = DBApp.getLastTrace("bvy");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_bvy[i])+")", tableTrace1.contains(Arrays.toString(records_bvy[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test78TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"fni","sx8","a0","a8225","rw6"};
		DBApp.createTable("o78", cols0);
		String [][] records_o78 = new String[89][cols0.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o78[i][j] = genRandString();
			}
			DBApp.insert("o78", records_o78[i]);
			String tableTrace0 = DBApp.getLastTrace("o78");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_o78[i])+")", tableTrace0.contains(Arrays.toString(records_o78[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"e06"};
		DBApp.createTable("u08", cols1);
		String [][] records_u08 = new String[86][cols1.length];
		for(int i=0;i<86;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_u08[i][j] = genRandString();
			}
			DBApp.insert("u08", records_u08[i]);
			String tableTrace1 = DBApp.getLastTrace("u08");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_u08[i])+")", tableTrace1.contains(Arrays.toString(records_u08[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"l3","t5h0","u375","xf8m","y63","hf5y","n5"};
		DBApp.createTable("h6z", cols2);
		String [][] records_h6z = new String[12][cols2.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_h6z[i][j] = genRandString();
			}
			DBApp.insert("h6z", records_h6z[i]);
			String tableTrace2 = DBApp.getLastTrace("h6z");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_h6z[i])+")", tableTrace2.contains(Arrays.toString(records_h6z[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"c2","i11","us","p4ex","ik6","mwl","o6q","hu5","ron","td874"};
		DBApp.createTable("s1r7", cols3);
		String [][] records_s1r7 = new String[43][cols3.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_s1r7[i][j] = genRandString();
			}
			DBApp.insert("s1r7", records_s1r7[i]);
			String tableTrace3 = DBApp.getLastTrace("s1r7");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_s1r7[i])+")", tableTrace3.contains(Arrays.toString(records_s1r7[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test79TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"c689z","m3w","d99","g3","hl","o93","e6","re","b6x"};
		DBApp.createTable("jce7", cols0);
		String [][] records_jce7 = new String[68][cols0.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_jce7[i][j] = genRandString();
			}
			DBApp.insert("jce7", records_jce7[i]);
			String tableTrace0 = DBApp.getLastTrace("jce7");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_jce7[i])+")", tableTrace0.contains(Arrays.toString(records_jce7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"o4pi","a24","a8","xy","z35","w3x17","qr","xbe","my","a48"};
		DBApp.createTable("kjv4g", cols1);
		String [][] records_kjv4g = new String[69][cols1.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_kjv4g[i][j] = genRandString();
			}
			DBApp.insert("kjv4g", records_kjv4g[i]);
			String tableTrace1 = DBApp.getLastTrace("kjv4g");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_kjv4g[i])+")", tableTrace1.contains(Arrays.toString(records_kjv4g[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"qd","b34","kr5u","e62","p55","qz9","upi","d0u","k0","x080"};
		DBApp.createTable("ii2", cols2);
		String [][] records_ii2 = new String[14][cols2.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ii2[i][j] = genRandString();
			}
			DBApp.insert("ii2", records_ii2[i]);
			String tableTrace2 = DBApp.getLastTrace("ii2");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ii2[i])+")", tableTrace2.contains(Arrays.toString(records_ii2[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"bh8t","b8","xr0","le9","jlp","t4k","n9c57","o31","zzn","j4e"};
		DBApp.createTable("xa7", cols3);
		String [][] records_xa7 = new String[43][cols3.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_xa7[i][j] = genRandString();
			}
			DBApp.insert("xa7", records_xa7[i]);
			String tableTrace3 = DBApp.getLastTrace("xa7");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_xa7[i])+")", tableTrace3.contains(Arrays.toString(records_xa7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"p8"};
		DBApp.createTable("x161", cols4);
		String [][] records_x161 = new String[48][cols4.length];
		for(int i=0;i<48;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_x161[i][j] = genRandString();
			}
			DBApp.insert("x161", records_x161[i]);
			String tableTrace4 = DBApp.getLastTrace("x161");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_x161[i])+")", tableTrace4.contains(Arrays.toString(records_x161[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test80TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"wxc","l5h1r"};
		DBApp.createTable("mq", cols0);
		String [][] records_mq = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_mq[i][j] = genRandString();
			}
			DBApp.insert("mq", records_mq[i]);
			String tableTrace0 = DBApp.getLastTrace("mq");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_mq[i])+")", tableTrace0.contains(Arrays.toString(records_mq[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"e9h7","n5e","p5"};
		DBApp.createTable("dq", cols1);
		String [][] records_dq = new String[41][cols1.length];
		for(int i=0;i<41;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_dq[i][j] = genRandString();
			}
			DBApp.insert("dq", records_dq[i]);
			String tableTrace1 = DBApp.getLastTrace("dq");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_dq[i])+")", tableTrace1.contains(Arrays.toString(records_dq[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"f4i","c0","f678","n4409","s42","tx2","e3","f22","vc7","rpcs8"};
		DBApp.createTable("sfd", cols2);
		String [][] records_sfd = new String[22][cols2.length];
		for(int i=0;i<22;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_sfd[i][j] = genRandString();
			}
			DBApp.insert("sfd", records_sfd[i]);
			String tableTrace2 = DBApp.getLastTrace("sfd");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_sfd[i])+")", tableTrace2.contains(Arrays.toString(records_sfd[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test81TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"r3","y8q","cs7","u1fha","ixrfr","gu","d5"};
		DBApp.createTable("ndxat", cols0);
		String [][] records_ndxat = new String[16][cols0.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ndxat[i][j] = genRandString();
			}
			DBApp.insert("ndxat", records_ndxat[i]);
			String tableTrace0 = DBApp.getLastTrace("ndxat");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ndxat[i])+")", tableTrace0.contains(Arrays.toString(records_ndxat[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"r3","e1"};
		DBApp.createTable("t18", cols1);
		String [][] records_t18 = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_t18[i][j] = genRandString();
			}
			DBApp.insert("t18", records_t18[i]);
			String tableTrace1 = DBApp.getLastTrace("t18");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_t18[i])+")", tableTrace1.contains(Arrays.toString(records_t18[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"i0","rtcy","cs25","a7186","x8","m61x"};
		DBApp.createTable("s88g", cols2);
		String [][] records_s88g = new String[77][cols2.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_s88g[i][j] = genRandString();
			}
			DBApp.insert("s88g", records_s88g[i]);
			String tableTrace2 = DBApp.getLastTrace("s88g");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_s88g[i])+")", tableTrace2.contains(Arrays.toString(records_s88g[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"i0g","ezc"};
		DBApp.createTable("a7nj8", cols3);
		String [][] records_a7nj8 = new String[99][cols3.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_a7nj8[i][j] = genRandString();
			}
			DBApp.insert("a7nj8", records_a7nj8[i]);
			String tableTrace3 = DBApp.getLastTrace("a7nj8");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_a7nj8[i])+")", tableTrace3.contains(Arrays.toString(records_a7nj8[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"l3u"};
		DBApp.createTable("nl567", cols4);
		String [][] records_nl567 = new String[40][cols4.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_nl567[i][j] = genRandString();
			}
			DBApp.insert("nl567", records_nl567[i]);
			String tableTrace4 = DBApp.getLastTrace("nl567");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_nl567[i])+")", tableTrace4.contains(Arrays.toString(records_nl567[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test82TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"bkqr0","mz","qqz02","lw1w","p310q","jva","cn","g5u6"};
		DBApp.createTable("nui3", cols0);
		String [][] records_nui3 = new String[30][cols0.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_nui3[i][j] = genRandString();
			}
			DBApp.insert("nui3", records_nui3[i]);
			String tableTrace0 = DBApp.getLastTrace("nui3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_nui3[i])+")", tableTrace0.contains(Arrays.toString(records_nui3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"vmnp","t51bl","h2a","h2","io2r"};
		DBApp.createTable("w0l9u", cols1);
		String [][] records_w0l9u = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_w0l9u[i][j] = genRandString();
			}
			DBApp.insert("w0l9u", records_w0l9u[i]);
			String tableTrace1 = DBApp.getLastTrace("w0l9u");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_w0l9u[i])+")", tableTrace1.contains(Arrays.toString(records_w0l9u[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"cvu","o4","odw"};
		DBApp.createTable("if9", cols2);
		String [][] records_if9 = new String[36][cols2.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_if9[i][j] = genRandString();
			}
			DBApp.insert("if9", records_if9[i]);
			String tableTrace2 = DBApp.getLastTrace("if9");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_if9[i])+")", tableTrace2.contains(Arrays.toString(records_if9[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"o0q","sk8c","is2hv"};
		DBApp.createTable("fh3", cols3);
		String [][] records_fh3 = new String[19][cols3.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_fh3[i][j] = genRandString();
			}
			DBApp.insert("fh3", records_fh3[i]);
			String tableTrace3 = DBApp.getLastTrace("fh3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_fh3[i])+")", tableTrace3.contains(Arrays.toString(records_fh3[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test83TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"ig9","n9","n7g","hg"};
		DBApp.createTable("fkz", cols0);
		String [][] records_fkz = new String[94][cols0.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_fkz[i][j] = genRandString();
			}
			DBApp.insert("fkz", records_fkz[i]);
			String tableTrace0 = DBApp.getLastTrace("fkz");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_fkz[i])+")", tableTrace0.contains(Arrays.toString(records_fkz[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"yy3v","h2p","hob","s5l"};
		DBApp.createTable("t3", cols1);
		String [][] records_t3 = new String[11][cols1.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_t3[i][j] = genRandString();
			}
			DBApp.insert("t3", records_t3[i]);
			String tableTrace1 = DBApp.getLastTrace("t3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_t3[i])+")", tableTrace1.contains(Arrays.toString(records_t3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"j200","yum","d3g","m352","cyx8g","xzn","h1z9","a49w","kz603","x66"};
		DBApp.createTable("u7k", cols2);
		String [][] records_u7k = new String[16][cols2.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_u7k[i][j] = genRandString();
			}
			DBApp.insert("u7k", records_u7k[i]);
			String tableTrace2 = DBApp.getLastTrace("u7k");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_u7k[i])+")", tableTrace2.contains(Arrays.toString(records_u7k[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"m45","z856v","ey4yz","qa8i","yywj7"};
		DBApp.createTable("no07", cols3);
		String [][] records_no07 = new String[82][cols3.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_no07[i][j] = genRandString();
			}
			DBApp.insert("no07", records_no07[i]);
			String tableTrace3 = DBApp.getLastTrace("no07");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_no07[i])+")", tableTrace3.contains(Arrays.toString(records_no07[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"jws","u68kq","ynw0j","gs0q","zt0","o1i0","ce1","lm5","o9r"};
		DBApp.createTable("dh39m", cols4);
		String [][] records_dh39m = new String[31][cols4.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_dh39m[i][j] = genRandString();
			}
			DBApp.insert("dh39m", records_dh39m[i]);
			String tableTrace4 = DBApp.getLastTrace("dh39m");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_dh39m[i])+")", tableTrace4.contains(Arrays.toString(records_dh39m[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test84TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"fl8"};
		DBApp.createTable("ib83", cols0);
		String [][] records_ib83 = new String[57][cols0.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ib83[i][j] = genRandString();
			}
			DBApp.insert("ib83", records_ib83[i]);
			String tableTrace0 = DBApp.getLastTrace("ib83");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ib83[i])+")", tableTrace0.contains(Arrays.toString(records_ib83[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"d6h22"};
		DBApp.createTable("b2", cols1);
		String [][] records_b2 = new String[44][cols1.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_b2[i][j] = genRandString();
			}
			DBApp.insert("b2", records_b2[i]);
			String tableTrace1 = DBApp.getLastTrace("b2");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_b2[i])+")", tableTrace1.contains(Arrays.toString(records_b2[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"yh","p981","e35","y8vk","ak","c76","j49"};
		DBApp.createTable("ke95", cols2);
		String [][] records_ke95 = new String[78][cols2.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ke95[i][j] = genRandString();
			}
			DBApp.insert("ke95", records_ke95[i]);
			String tableTrace2 = DBApp.getLastTrace("ke95");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ke95[i])+")", tableTrace2.contains(Arrays.toString(records_ke95[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"i60"};
		DBApp.createTable("xm", cols3);
		String [][] records_xm = new String[47][cols3.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_xm[i][j] = genRandString();
			}
			DBApp.insert("xm", records_xm[i]);
			String tableTrace3 = DBApp.getLastTrace("xm");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_xm[i])+")", tableTrace3.contains(Arrays.toString(records_xm[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test85TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"wh5","hdy","gtl6","a7815"};
		DBApp.createTable("j0", cols0);
		String [][] records_j0 = new String[38][cols0.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_j0[i][j] = genRandString();
			}
			DBApp.insert("j0", records_j0[i]);
			String tableTrace0 = DBApp.getLastTrace("j0");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_j0[i])+")", tableTrace0.contains(Arrays.toString(records_j0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"iz0","n8293","ay","r96","r8c0","g3999","e3947"};
		DBApp.createTable("b7", cols1);
		String [][] records_b7 = new String[2][cols1.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_b7[i][j] = genRandString();
			}
			DBApp.insert("b7", records_b7[i]);
			String tableTrace1 = DBApp.getLastTrace("b7");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_b7[i])+")", tableTrace1.contains(Arrays.toString(records_b7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"k67","s2d26","e6a","w443","e871p","m0744"};
		DBApp.createTable("cb8", cols2);
		String [][] records_cb8 = new String[94][cols2.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_cb8[i][j] = genRandString();
			}
			DBApp.insert("cb8", records_cb8[i]);
			String tableTrace2 = DBApp.getLastTrace("cb8");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_cb8[i])+")", tableTrace2.contains(Arrays.toString(records_cb8[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"w905","kz","j3p","rg","v40f","ry17","w7","nw","r3g","ix"};
		DBApp.createTable("ce6", cols3);
		String [][] records_ce6 = new String[49][cols3.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ce6[i][j] = genRandString();
			}
			DBApp.insert("ce6", records_ce6[i]);
			String tableTrace3 = DBApp.getLastTrace("ce6");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ce6[i])+")", tableTrace3.contains(Arrays.toString(records_ce6[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test86TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"be","wk4"};
		DBApp.createTable("q1b", cols0);
		String [][] records_q1b = new String[46][cols0.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_q1b[i][j] = genRandString();
			}
			DBApp.insert("q1b", records_q1b[i]);
			String tableTrace0 = DBApp.getLastTrace("q1b");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_q1b[i])+")", tableTrace0.contains(Arrays.toString(records_q1b[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"k746","s38","lc8","zb","l8p","trt","n7o","ax9","r0k","d39z3"};
		DBApp.createTable("ofk", cols1);
		String [][] records_ofk = new String[21][cols1.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ofk[i][j] = genRandString();
			}
			DBApp.insert("ofk", records_ofk[i]);
			String tableTrace1 = DBApp.getLastTrace("ofk");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ofk[i])+")", tableTrace1.contains(Arrays.toString(records_ofk[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test87TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"prquc","c1b","pm4w","w3y","l6f","hw2","gd","hwkk"};
		DBApp.createTable("n7p4s", cols0);
		String [][] records_n7p4s = new String[33][cols0.length];
		for(int i=0;i<33;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_n7p4s[i][j] = genRandString();
			}
			DBApp.insert("n7p4s", records_n7p4s[i]);
			String tableTrace0 = DBApp.getLastTrace("n7p4s");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_n7p4s[i])+")", tableTrace0.contains(Arrays.toString(records_n7p4s[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test88TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"t6","l53","bt","zk","dih","j68l6","i40"};
		DBApp.createTable("sn", cols0);
		String [][] records_sn = new String[78][cols0.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sn[i][j] = genRandString();
			}
			DBApp.insert("sn", records_sn[i]);
			String tableTrace0 = DBApp.getLastTrace("sn");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_sn[i])+")", tableTrace0.contains(Arrays.toString(records_sn[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"c9","e9k","awn70","uxx","uky","r33"};
		DBApp.createTable("rem", cols1);
		String [][] records_rem = new String[24][cols1.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_rem[i][j] = genRandString();
			}
			DBApp.insert("rem", records_rem[i]);
			String tableTrace1 = DBApp.getLastTrace("rem");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_rem[i])+")", tableTrace1.contains(Arrays.toString(records_rem[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"x3"};
		DBApp.createTable("jnt2s", cols2);
		String [][] records_jnt2s = new String[16][cols2.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_jnt2s[i][j] = genRandString();
			}
			DBApp.insert("jnt2s", records_jnt2s[i]);
			String tableTrace2 = DBApp.getLastTrace("jnt2s");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_jnt2s[i])+")", tableTrace2.contains(Arrays.toString(records_jnt2s[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"m68"};
		DBApp.createTable("qoy4", cols3);
		String [][] records_qoy4 = new String[27][cols3.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_qoy4[i][j] = genRandString();
			}
			DBApp.insert("qoy4", records_qoy4[i]);
			String tableTrace3 = DBApp.getLastTrace("qoy4");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_qoy4[i])+")", tableTrace3.contains(Arrays.toString(records_qoy4[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test89TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"fbk","i14","s53","am6","x36mx","w14","ba59","p26"};
		DBApp.createTable("ld2", cols0);
		String [][] records_ld2 = new String[14][cols0.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ld2[i][j] = genRandString();
			}
			DBApp.insert("ld2", records_ld2[i]);
			String tableTrace0 = DBApp.getLastTrace("ld2");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ld2[i])+")", tableTrace0.contains(Arrays.toString(records_ld2[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test90TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"im7","i1h2q","h49","f04tc","l6vd1","pc8i"};
		DBApp.createTable("vf", cols0);
		String [][] records_vf = new String[23][cols0.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vf[i][j] = genRandString();
			}
			DBApp.insert("vf", records_vf[i]);
			String tableTrace0 = DBApp.getLastTrace("vf");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_vf[i])+")", tableTrace0.contains(Arrays.toString(records_vf[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test91TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"us","ssg","yx7","o30","t3um1","k1","k2","o6p78","rh8"};
		DBApp.createTable("yr3", cols0);
		String [][] records_yr3 = new String[56][cols0.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yr3[i][j] = genRandString();
			}
			DBApp.insert("yr3", records_yr3[i]);
			String tableTrace0 = DBApp.getLastTrace("yr3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_yr3[i])+")", tableTrace0.contains(Arrays.toString(records_yr3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"ld","ao1","c39i","x3f"};
		DBApp.createTable("j4n", cols1);
		String [][] records_j4n = new String[29][cols1.length];
		for(int i=0;i<29;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_j4n[i][j] = genRandString();
			}
			DBApp.insert("j4n", records_j4n[i]);
			String tableTrace1 = DBApp.getLastTrace("j4n");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_j4n[i])+")", tableTrace1.contains(Arrays.toString(records_j4n[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"lryv4","u7","gj","t5x73","b8e3","cmz","s2l","bqk","m7226","lh63a"};
		DBApp.createTable("fi", cols2);
		String [][] records_fi = new String[50][cols2.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_fi[i][j] = genRandString();
			}
			DBApp.insert("fi", records_fi[i]);
			String tableTrace2 = DBApp.getLastTrace("fi");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_fi[i])+")", tableTrace2.contains(Arrays.toString(records_fi[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"c5c6","lw3","b7a","qz68","uwd"};
		DBApp.createTable("sr4n1", cols3);
		String [][] records_sr4n1 = new String[90][cols3.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_sr4n1[i][j] = genRandString();
			}
			DBApp.insert("sr4n1", records_sr4n1[i]);
			String tableTrace3 = DBApp.getLastTrace("sr4n1");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_sr4n1[i])+")", tableTrace3.contains(Arrays.toString(records_sr4n1[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"i7u","zc","xb79e","k18tq","nr3"};
		DBApp.createTable("x6y", cols4);
		String [][] records_x6y = new String[55][cols4.length];
		for(int i=0;i<55;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_x6y[i][j] = genRandString();
			}
			DBApp.insert("x6y", records_x6y[i]);
			String tableTrace4 = DBApp.getLastTrace("x6y");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_x6y[i])+")", tableTrace4.contains(Arrays.toString(records_x6y[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test92TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"lip","yc2b2","j9l7h","ikjv","e74f7","fs","bi","fv","l07m","x2l36"};
		DBApp.createTable("p8y", cols0);
		String [][] records_p8y = new String[88][cols0.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p8y[i][j] = genRandString();
			}
			DBApp.insert("p8y", records_p8y[i]);
			String tableTrace0 = DBApp.getLastTrace("p8y");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_p8y[i])+")", tableTrace0.contains(Arrays.toString(records_p8y[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"o4","mvtw","c90","pt3"};
		DBApp.createTable("u49k7", cols1);
		String [][] records_u49k7 = new String[65][cols1.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_u49k7[i][j] = genRandString();
			}
			DBApp.insert("u49k7", records_u49k7[i]);
			String tableTrace1 = DBApp.getLastTrace("u49k7");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_u49k7[i])+")", tableTrace1.contains(Arrays.toString(records_u49k7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"ae","b89","a8b","o1a8k","az766","ev3k8","dq2"};
		DBApp.createTable("mnl5", cols2);
		String [][] records_mnl5 = new String[40][cols2.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_mnl5[i][j] = genRandString();
			}
			DBApp.insert("mnl5", records_mnl5[i]);
			String tableTrace2 = DBApp.getLastTrace("mnl5");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_mnl5[i])+")", tableTrace2.contains(Arrays.toString(records_mnl5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"ew","jw","qw","a6p18","vq","t08","amg","ng4","wi","xegg"};
		DBApp.createTable("ij6", cols3);
		String [][] records_ij6 = new String[17][cols3.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ij6[i][j] = genRandString();
			}
			DBApp.insert("ij6", records_ij6[i]);
			String tableTrace3 = DBApp.getLastTrace("ij6");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ij6[i])+")", tableTrace3.contains(Arrays.toString(records_ij6[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"tf","qp81","b3r1","e5411","uf3n","y8","ld63"};
		DBApp.createTable("wt76q", cols4);
		String [][] records_wt76q = new String[41][cols4.length];
		for(int i=0;i<41;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_wt76q[i][j] = genRandString();
			}
			DBApp.insert("wt76q", records_wt76q[i]);
			String tableTrace4 = DBApp.getLastTrace("wt76q");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_wt76q[i])+")", tableTrace4.contains(Arrays.toString(records_wt76q[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test93TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"ru0","f11k","nti","mpf3","t8e","t9b","av","mq","vz","l0"};
		DBApp.createTable("du7", cols0);
		String [][] records_du7 = new String[82][cols0.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_du7[i][j] = genRandString();
			}
			DBApp.insert("du7", records_du7[i]);
			String tableTrace0 = DBApp.getLastTrace("du7");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_du7[i])+")", tableTrace0.contains(Arrays.toString(records_du7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"ta6","yqf","ua7e","z06"};
		DBApp.createTable("st679", cols1);
		String [][] records_st679 = new String[37][cols1.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_st679[i][j] = genRandString();
			}
			DBApp.insert("st679", records_st679[i]);
			String tableTrace1 = DBApp.getLastTrace("st679");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_st679[i])+")", tableTrace1.contains(Arrays.toString(records_st679[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"jh","h4","slz6","jx87","g1q0","qgexn"};
		DBApp.createTable("x2z", cols2);
		String [][] records_x2z = new String[1][cols2.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_x2z[i][j] = genRandString();
			}
			DBApp.insert("x2z", records_x2z[i]);
			String tableTrace2 = DBApp.getLastTrace("x2z");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_x2z[i])+")", tableTrace2.contains(Arrays.toString(records_x2z[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"ihl","wu","pxi8i","fi2","o551","sv6j9","m99px","v0ec"};
		DBApp.createTable("djg00", cols3);
		String [][] records_djg00 = new String[98][cols3.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_djg00[i][j] = genRandString();
			}
			DBApp.insert("djg00", records_djg00[i]);
			String tableTrace3 = DBApp.getLastTrace("djg00");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_djg00[i])+")", tableTrace3.contains(Arrays.toString(records_djg00[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"p9","v3","f29","e4"};
		DBApp.createTable("guhd", cols4);
		String [][] records_guhd = new String[81][cols4.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_guhd[i][j] = genRandString();
			}
			DBApp.insert("guhd", records_guhd[i]);
			String tableTrace4 = DBApp.getLastTrace("guhd");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_guhd[i])+")", tableTrace4.contains(Arrays.toString(records_guhd[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test94TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"wc0g","t46","k7o"};
		DBApp.createTable("a41v", cols0);
		String [][] records_a41v = new String[31][cols0.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_a41v[i][j] = genRandString();
			}
			DBApp.insert("a41v", records_a41v[i]);
			String tableTrace0 = DBApp.getLastTrace("a41v");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_a41v[i])+")", tableTrace0.contains(Arrays.toString(records_a41v[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"m3","v1","m9t","u10p"};
		DBApp.createTable("no", cols1);
		String [][] records_no = new String[99][cols1.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_no[i][j] = genRandString();
			}
			DBApp.insert("no", records_no[i]);
			String tableTrace1 = DBApp.getLastTrace("no");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_no[i])+")", tableTrace1.contains(Arrays.toString(records_no[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test95TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"f6w","c03t"};
		DBApp.createTable("p2s", cols0);
		String [][] records_p2s = new String[81][cols0.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p2s[i][j] = genRandString();
			}
			DBApp.insert("p2s", records_p2s[i]);
			String tableTrace0 = DBApp.getLastTrace("p2s");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_p2s[i])+")", tableTrace0.contains(Arrays.toString(records_p2s[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test96TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"e5d","j9","m42x","e0","m3i","ap8jv"};
		DBApp.createTable("na2b", cols0);
		String [][] records_na2b = new String[81][cols0.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_na2b[i][j] = genRandString();
			}
			DBApp.insert("na2b", records_na2b[i]);
			String tableTrace0 = DBApp.getLastTrace("na2b");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_na2b[i])+")", tableTrace0.contains(Arrays.toString(records_na2b[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test97TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"s5tc4","ktg2","q74","a76","xp082","d4hf9"};
		DBApp.createTable("u26", cols0);
		String [][] records_u26 = new String[41][cols0.length];
		for(int i=0;i<41;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u26[i][j] = genRandString();
			}
			DBApp.insert("u26", records_u26[i]);
			String tableTrace0 = DBApp.getLastTrace("u26");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_u26[i])+")", tableTrace0.contains(Arrays.toString(records_u26[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"pq1","m48","k0p","l2","zgs3","ns"};
		DBApp.createTable("l13", cols1);
		String [][] records_l13 = new String[87][cols1.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_l13[i][j] = genRandString();
			}
			DBApp.insert("l13", records_l13[i]);
			String tableTrace1 = DBApp.getLastTrace("l13");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_l13[i])+")", tableTrace1.contains(Arrays.toString(records_l13[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"v8ofm"};
		DBApp.createTable("l45", cols2);
		String [][] records_l45 = new String[89][cols2.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_l45[i][j] = genRandString();
			}
			DBApp.insert("l45", records_l45[i]);
			String tableTrace2 = DBApp.getLastTrace("l45");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_l45[i])+")", tableTrace2.contains(Arrays.toString(records_l45[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"qlg78","hr","d9","p5es","ubh","f15","z04","w51"};
		DBApp.createTable("cf0", cols3);
		String [][] records_cf0 = new String[57][cols3.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_cf0[i][j] = genRandString();
			}
			DBApp.insert("cf0", records_cf0[i]);
			String tableTrace3 = DBApp.getLastTrace("cf0");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_cf0[i])+")", tableTrace3.contains(Arrays.toString(records_cf0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"j7","j4mv","osr","kq"};
		DBApp.createTable("li", cols4);
		String [][] records_li = new String[100][cols4.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_li[i][j] = genRandString();
			}
			DBApp.insert("li", records_li[i]);
			String tableTrace4 = DBApp.getLastTrace("li");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_li[i])+")", tableTrace4.contains(Arrays.toString(records_li[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test98TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"v577","k23","f06","j96"};
		DBApp.createTable("x01", cols0);
		String [][] records_x01 = new String[70][cols0.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x01[i][j] = genRandString();
			}
			DBApp.insert("x01", records_x01[i]);
			String tableTrace0 = DBApp.getLastTrace("x01");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_x01[i])+")", tableTrace0.contains(Arrays.toString(records_x01[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"h03rj","pld","hnu5","hjkv","bf5k0","n4u","s26","kh9o"};
		DBApp.createTable("q1", cols1);
		String [][] records_q1 = new String[42][cols1.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_q1[i][j] = genRandString();
			}
			DBApp.insert("q1", records_q1[i]);
			String tableTrace1 = DBApp.getLastTrace("q1");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_q1[i])+")", tableTrace1.contains(Arrays.toString(records_q1[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"u2ttl","s5f"};
		DBApp.createTable("o43", cols2);
		String [][] records_o43 = new String[59][cols2.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_o43[i][j] = genRandString();
			}
			DBApp.insert("o43", records_o43[i]);
			String tableTrace2 = DBApp.getLastTrace("o43");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_o43[i])+")", tableTrace2.contains(Arrays.toString(records_o43[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"jodt","xj","hkcl9","xd","tg","l8u","o7g"};
		DBApp.createTable("i48ii", cols3);
		String [][] records_i48ii = new String[50][cols3.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_i48ii[i][j] = genRandString();
			}
			DBApp.insert("i48ii", records_i48ii[i]);
			String tableTrace3 = DBApp.getLastTrace("i48ii");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_i48ii[i])+")", tableTrace3.contains(Arrays.toString(records_i48ii[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test99TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"g787","em0","pho","b2"};
		DBApp.createTable("sj2", cols0);
		String [][] records_sj2 = new String[33][cols0.length];
		for(int i=0;i<33;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sj2[i][j] = genRandString();
			}
			DBApp.insert("sj2", records_sj2[i]);
			String tableTrace0 = DBApp.getLastTrace("sj2");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_sj2[i])+")", tableTrace0.contains(Arrays.toString(records_sj2[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"v2","pu","qp2cz","d1e","f886","hqr0a"};
		DBApp.createTable("y570", cols1);
		String [][] records_y570 = new String[75][cols1.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_y570[i][j] = genRandString();
			}
			DBApp.insert("y570", records_y570[i]);
			String tableTrace1 = DBApp.getLastTrace("y570");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_y570[i])+")", tableTrace1.contains(Arrays.toString(records_y570[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"lh6","eqi","fp","qm7","y5"};
		DBApp.createTable("yk", cols2);
		String [][] records_yk = new String[25][cols2.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_yk[i][j] = genRandString();
			}
			DBApp.insert("yk", records_yk[i]);
			String tableTrace2 = DBApp.getLastTrace("yk");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_yk[i])+")", tableTrace2.contains(Arrays.toString(records_yk[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a61","gib","s50","n5","zm4j","i8","d0a","ld4x","fm","p43p"};
		DBApp.createTable("f46un", cols3);
		String [][] records_f46un = new String[51][cols3.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_f46un[i][j] = genRandString();
			}
			DBApp.insert("f46un", records_f46un[i]);
			String tableTrace3 = DBApp.getLastTrace("f46un");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_f46un[i])+")", tableTrace3.contains(Arrays.toString(records_f46un[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"t3t","f736","db9j1","x74"};
		DBApp.createTable("xl", cols4);
		String [][] records_xl = new String[95][cols4.length];
		for(int i=0;i<95;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_xl[i][j] = genRandString();
			}
			DBApp.insert("xl", records_xl[i]);
			String tableTrace4 = DBApp.getLastTrace("xl");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_xl[i])+")", tableTrace4.contains(Arrays.toString(records_xl[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test100TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"j724","xhh","t0","u0w","z1","tk","pauv","res54","zklm"};
		DBApp.createTable("oy8", cols0);
		String [][] records_oy8 = new String[79][cols0.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_oy8[i][j] = genRandString();
			}
			DBApp.insert("oy8", records_oy8[i]);
			String tableTrace0 = DBApp.getLastTrace("oy8");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_oy8[i])+")", tableTrace0.contains(Arrays.toString(records_oy8[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"d1mr","rya"};
		DBApp.createTable("rn2", cols1);
		String [][] records_rn2 = new String[78][cols1.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_rn2[i][j] = genRandString();
			}
			DBApp.insert("rn2", records_rn2[i]);
			String tableTrace1 = DBApp.getLastTrace("rn2");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_rn2[i])+")", tableTrace1.contains(Arrays.toString(records_rn2[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test101TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"x4l","j9","f3k","hb","v5g","f0k7","d4726","a0j0j","c0","q4"};
		DBApp.createTable("o6ez", cols0);
		String [][] records_o6ez = new String[6][cols0.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o6ez[i][j] = genRandString();
			}
			DBApp.insert("o6ez", records_o6ez[i]);
			String tableTrace0 = DBApp.getLastTrace("o6ez");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_o6ez[i])+")", tableTrace0.contains(Arrays.toString(records_o6ez[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"w2t","d8519"};
		DBApp.createTable("t3r", cols1);
		String [][] records_t3r = new String[97][cols1.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_t3r[i][j] = genRandString();
			}
			DBApp.insert("t3r", records_t3r[i]);
			String tableTrace1 = DBApp.getLastTrace("t3r");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_t3r[i])+")", tableTrace1.contains(Arrays.toString(records_t3r[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"u19c9"};
		DBApp.createTable("m59", cols2);
		String [][] records_m59 = new String[96][cols2.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_m59[i][j] = genRandString();
			}
			DBApp.insert("m59", records_m59[i]);
			String tableTrace2 = DBApp.getLastTrace("m59");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_m59[i])+")", tableTrace2.contains(Arrays.toString(records_m59[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test102TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"nww0","z9g","xi2","l4s1","xn","h9vi","mn3","t55","gd"};
		DBApp.createTable("aj", cols0);
		String [][] records_aj = new String[19][cols0.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_aj[i][j] = genRandString();
			}
			DBApp.insert("aj", records_aj[i]);
			String tableTrace0 = DBApp.getLastTrace("aj");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_aj[i])+")", tableTrace0.contains(Arrays.toString(records_aj[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"z291","tnv9","cf9i0","v5w","l3je","a7","kml"};
		DBApp.createTable("st9", cols1);
		String [][] records_st9 = new String[35][cols1.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_st9[i][j] = genRandString();
			}
			DBApp.insert("st9", records_st9[i]);
			String tableTrace1 = DBApp.getLastTrace("st9");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_st9[i])+")", tableTrace1.contains(Arrays.toString(records_st9[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"m6","c55wt","a5b87","j090","ih3f9","iz","h1","f94"};
		DBApp.createTable("vy990", cols2);
		String [][] records_vy990 = new String[12][cols2.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_vy990[i][j] = genRandString();
			}
			DBApp.insert("vy990", records_vy990[i]);
			String tableTrace2 = DBApp.getLastTrace("vy990");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_vy990[i])+")", tableTrace2.contains(Arrays.toString(records_vy990[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"dck"};
		DBApp.createTable("t98", cols3);
		String [][] records_t98 = new String[6][cols3.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_t98[i][j] = genRandString();
			}
			DBApp.insert("t98", records_t98[i]);
			String tableTrace3 = DBApp.getLastTrace("t98");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_t98[i])+")", tableTrace3.contains(Arrays.toString(records_t98[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test103TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"l4"};
		DBApp.createTable("j0", cols0);
		String [][] records_j0 = new String[17][cols0.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_j0[i][j] = genRandString();
			}
			DBApp.insert("j0", records_j0[i]);
			String tableTrace0 = DBApp.getLastTrace("j0");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_j0[i])+")", tableTrace0.contains(Arrays.toString(records_j0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"omll","j33","i908"};
		DBApp.createTable("c4", cols1);
		String [][] records_c4 = new String[66][cols1.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_c4[i][j] = genRandString();
			}
			DBApp.insert("c4", records_c4[i]);
			String tableTrace1 = DBApp.getLastTrace("c4");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_c4[i])+")", tableTrace1.contains(Arrays.toString(records_c4[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test104TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"qtv68","yp","czo50","p517"};
		DBApp.createTable("ajnn", cols0);
		String [][] records_ajnn = new String[46][cols0.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ajnn[i][j] = genRandString();
			}
			DBApp.insert("ajnn", records_ajnn[i]);
			String tableTrace0 = DBApp.getLastTrace("ajnn");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ajnn[i])+")", tableTrace0.contains(Arrays.toString(records_ajnn[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"m6s","b6w","cix","vx","xp6","xlm"};
		DBApp.createTable("zc", cols1);
		String [][] records_zc = new String[14][cols1.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_zc[i][j] = genRandString();
			}
			DBApp.insert("zc", records_zc[i]);
			String tableTrace1 = DBApp.getLastTrace("zc");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_zc[i])+")", tableTrace1.contains(Arrays.toString(records_zc[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"x476r"};
		DBApp.createTable("jif", cols2);
		String [][] records_jif = new String[35][cols2.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_jif[i][j] = genRandString();
			}
			DBApp.insert("jif", records_jif[i]);
			String tableTrace2 = DBApp.getLastTrace("jif");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_jif[i])+")", tableTrace2.contains(Arrays.toString(records_jif[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test105TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"i1o","r6sz","svl2","sa6","e2w","wp","k1","x1"};
		DBApp.createTable("y10", cols0);
		String [][] records_y10 = new String[48][cols0.length];
		for(int i=0;i<48;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_y10[i][j] = genRandString();
			}
			DBApp.insert("y10", records_y10[i]);
			String tableTrace0 = DBApp.getLastTrace("y10");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_y10[i])+")", tableTrace0.contains(Arrays.toString(records_y10[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"toy8","o30","q84","odvx","mz745"};
		DBApp.createTable("wt775", cols1);
		String [][] records_wt775 = new String[90][cols1.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_wt775[i][j] = genRandString();
			}
			DBApp.insert("wt775", records_wt775[i]);
			String tableTrace1 = DBApp.getLastTrace("wt775");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_wt775[i])+")", tableTrace1.contains(Arrays.toString(records_wt775[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"ev","y15","p75jf","k3v10","rf64a","jkx","s430","ge8"};
		DBApp.createTable("tmv8", cols2);
		String [][] records_tmv8 = new String[11][cols2.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_tmv8[i][j] = genRandString();
			}
			DBApp.insert("tmv8", records_tmv8[i]);
			String tableTrace2 = DBApp.getLastTrace("tmv8");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_tmv8[i])+")", tableTrace2.contains(Arrays.toString(records_tmv8[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test106TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"o7h"};
		DBApp.createTable("q3i2u", cols0);
		String [][] records_q3i2u = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_q3i2u[i][j] = genRandString();
			}
			DBApp.insert("q3i2u", records_q3i2u[i]);
			String tableTrace0 = DBApp.getLastTrace("q3i2u");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_q3i2u[i])+")", tableTrace0.contains(Arrays.toString(records_q3i2u[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"ux6","i7a"};
		DBApp.createTable("y4aoj", cols1);
		String [][] records_y4aoj = new String[85][cols1.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_y4aoj[i][j] = genRandString();
			}
			DBApp.insert("y4aoj", records_y4aoj[i]);
			String tableTrace1 = DBApp.getLastTrace("y4aoj");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_y4aoj[i])+")", tableTrace1.contains(Arrays.toString(records_y4aoj[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"x1g","t8","c976q","lcb","ndqoo","k0823"};
		DBApp.createTable("bo", cols2);
		String [][] records_bo = new String[28][cols2.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_bo[i][j] = genRandString();
			}
			DBApp.insert("bo", records_bo[i]);
			String tableTrace2 = DBApp.getLastTrace("bo");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_bo[i])+")", tableTrace2.contains(Arrays.toString(records_bo[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"j2y7k","u0l","m7","nfz","yz3n","zf6c8","v5x","d689l","mr652","xha"};
		DBApp.createTable("povf", cols3);
		String [][] records_povf = new String[56][cols3.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_povf[i][j] = genRandString();
			}
			DBApp.insert("povf", records_povf[i]);
			String tableTrace3 = DBApp.getLastTrace("povf");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_povf[i])+")", tableTrace3.contains(Arrays.toString(records_povf[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"f9or","vmqqj"};
		DBApp.createTable("jrr8f", cols4);
		String [][] records_jrr8f = new String[49][cols4.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_jrr8f[i][j] = genRandString();
			}
			DBApp.insert("jrr8f", records_jrr8f[i]);
			String tableTrace4 = DBApp.getLastTrace("jrr8f");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_jrr8f[i])+")", tableTrace4.contains(Arrays.toString(records_jrr8f[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test107TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"ni","ryx","l7","ee4","br9","f68","mp","w3co","o0l86"};
		DBApp.createTable("n8", cols0);
		String [][] records_n8 = new String[35][cols0.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_n8[i][j] = genRandString();
			}
			DBApp.insert("n8", records_n8[i]);
			String tableTrace0 = DBApp.getLastTrace("n8");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_n8[i])+")", tableTrace0.contains(Arrays.toString(records_n8[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"e31"};
		DBApp.createTable("x926", cols1);
		String [][] records_x926 = new String[25][cols1.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_x926[i][j] = genRandString();
			}
			DBApp.insert("x926", records_x926[i]);
			String tableTrace1 = DBApp.getLastTrace("x926");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_x926[i])+")", tableTrace1.contains(Arrays.toString(records_x926[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test108TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"s13s1","r57","jn74","axw","r2","p3b61","nxv","s5","k718c"};
		DBApp.createTable("q8", cols0);
		String [][] records_q8 = new String[31][cols0.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_q8[i][j] = genRandString();
			}
			DBApp.insert("q8", records_q8[i]);
			String tableTrace0 = DBApp.getLastTrace("q8");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_q8[i])+")", tableTrace0.contains(Arrays.toString(records_q8[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"nt8","bhi","el","i16","xj","s69kc","u44","p1s","smcqi"};
		DBApp.createTable("u415", cols1);
		String [][] records_u415 = new String[96][cols1.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_u415[i][j] = genRandString();
			}
			DBApp.insert("u415", records_u415[i]);
			String tableTrace1 = DBApp.getLastTrace("u415");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_u415[i])+")", tableTrace1.contains(Arrays.toString(records_u415[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"dd1d1","z6q","rnz82"};
		DBApp.createTable("od", cols2);
		String [][] records_od = new String[75][cols2.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_od[i][j] = genRandString();
			}
			DBApp.insert("od", records_od[i]);
			String tableTrace2 = DBApp.getLastTrace("od");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_od[i])+")", tableTrace2.contains(Arrays.toString(records_od[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"j225","bva","wrk72"};
		DBApp.createTable("hupf", cols3);
		String [][] records_hupf = new String[47][cols3.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_hupf[i][j] = genRandString();
			}
			DBApp.insert("hupf", records_hupf[i]);
			String tableTrace3 = DBApp.getLastTrace("hupf");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_hupf[i])+")", tableTrace3.contains(Arrays.toString(records_hupf[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test109TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"yz7","qi4","n3c","ay1u","i0m9z","v04va","s2e","jz9"};
		DBApp.createTable("gubz", cols0);
		String [][] records_gubz = new String[13][cols0.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_gubz[i][j] = genRandString();
			}
			DBApp.insert("gubz", records_gubz[i]);
			String tableTrace0 = DBApp.getLastTrace("gubz");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_gubz[i])+")", tableTrace0.contains(Arrays.toString(records_gubz[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"dc7","h4","u2p","t9a","c9y","equ"};
		DBApp.createTable("rw51", cols1);
		String [][] records_rw51 = new String[22][cols1.length];
		for(int i=0;i<22;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_rw51[i][j] = genRandString();
			}
			DBApp.insert("rw51", records_rw51[i]);
			String tableTrace1 = DBApp.getLastTrace("rw51");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_rw51[i])+")", tableTrace1.contains(Arrays.toString(records_rw51[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"qkk39","hw","xnxz1","j44g"};
		DBApp.createTable("gg982", cols2);
		String [][] records_gg982 = new String[28][cols2.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_gg982[i][j] = genRandString();
			}
			DBApp.insert("gg982", records_gg982[i]);
			String tableTrace2 = DBApp.getLastTrace("gg982");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_gg982[i])+")", tableTrace2.contains(Arrays.toString(records_gg982[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test110TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"n86","ko","ky"};
		DBApp.createTable("gsyb9", cols0);
		String [][] records_gsyb9 = new String[73][cols0.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_gsyb9[i][j] = genRandString();
			}
			DBApp.insert("gsyb9", records_gsyb9[i]);
			String tableTrace0 = DBApp.getLastTrace("gsyb9");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_gsyb9[i])+")", tableTrace0.contains(Arrays.toString(records_gsyb9[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test111TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"niq","br0","k4s88","n8kt2","nwx","pw1","dwd7","v4","w3w"};
		DBApp.createTable("noit", cols0);
		String [][] records_noit = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_noit[i][j] = genRandString();
			}
			DBApp.insert("noit", records_noit[i]);
			String tableTrace0 = DBApp.getLastTrace("noit");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_noit[i])+")", tableTrace0.contains(Arrays.toString(records_noit[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test112TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"px","cf1o","ex7","gq8"};
		DBApp.createTable("h2eg4", cols0);
		String [][] records_h2eg4 = new String[8][cols0.length];
		for(int i=0;i<8;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h2eg4[i][j] = genRandString();
			}
			DBApp.insert("h2eg4", records_h2eg4[i]);
			String tableTrace0 = DBApp.getLastTrace("h2eg4");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_h2eg4[i])+")", tableTrace0.contains(Arrays.toString(records_h2eg4[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test113TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"ll","r2w","b37c4","bv7q","a9b","wu33x","mgn2j","i9gk1"};
		DBApp.createTable("rjp", cols0);
		String [][] records_rjp = new String[82][cols0.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_rjp[i][j] = genRandString();
			}
			DBApp.insert("rjp", records_rjp[i]);
			String tableTrace0 = DBApp.getLastTrace("rjp");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_rjp[i])+")", tableTrace0.contains(Arrays.toString(records_rjp[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test114TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"x5","v6","p64","t9u"};
		DBApp.createTable("a6x", cols0);
		String [][] records_a6x = new String[42][cols0.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_a6x[i][j] = genRandString();
			}
			DBApp.insert("a6x", records_a6x[i]);
			String tableTrace0 = DBApp.getLastTrace("a6x");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_a6x[i])+")", tableTrace0.contains(Arrays.toString(records_a6x[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"v90","y37","jju2","mgxp","m73"};
		DBApp.createTable("z0", cols1);
		String [][] records_z0 = new String[34][cols1.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_z0[i][j] = genRandString();
			}
			DBApp.insert("z0", records_z0[i]);
			String tableTrace1 = DBApp.getLastTrace("z0");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_z0[i])+")", tableTrace1.contains(Arrays.toString(records_z0[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test115TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"tj1r0","e8w","w7qo","y4m","tmqjl","h1sd9","q294","t5","wjep"};
		DBApp.createTable("r4", cols0);
		String [][] records_r4 = new String[20][cols0.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r4[i][j] = genRandString();
			}
			DBApp.insert("r4", records_r4[i]);
			String tableTrace0 = DBApp.getLastTrace("r4");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_r4[i])+")", tableTrace0.contains(Arrays.toString(records_r4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"nf7","qa1","ge3j","nz0","g1l","fr","ucvym","v7"};
		DBApp.createTable("z8t", cols1);
		String [][] records_z8t = new String[87][cols1.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_z8t[i][j] = genRandString();
			}
			DBApp.insert("z8t", records_z8t[i]);
			String tableTrace1 = DBApp.getLastTrace("z8t");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_z8t[i])+")", tableTrace1.contains(Arrays.toString(records_z8t[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"cxd","j5j","v591","j2w","m4p","vt0"};
		DBApp.createTable("j5ola", cols2);
		String [][] records_j5ola = new String[100][cols2.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_j5ola[i][j] = genRandString();
			}
			DBApp.insert("j5ola", records_j5ola[i]);
			String tableTrace2 = DBApp.getLastTrace("j5ola");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_j5ola[i])+")", tableTrace2.contains(Arrays.toString(records_j5ola[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"p15","g0u","i1d0","fyg3","rto"};
		DBApp.createTable("wy", cols3);
		String [][] records_wy = new String[85][cols3.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_wy[i][j] = genRandString();
			}
			DBApp.insert("wy", records_wy[i]);
			String tableTrace3 = DBApp.getLastTrace("wy");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_wy[i])+")", tableTrace3.contains(Arrays.toString(records_wy[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test116TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"f1","y64","k32","qcpzr","cm","etq","djpji","la7"};
		DBApp.createTable("a51w5", cols0);
		String [][] records_a51w5 = new String[99][cols0.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_a51w5[i][j] = genRandString();
			}
			DBApp.insert("a51w5", records_a51w5[i]);
			String tableTrace0 = DBApp.getLastTrace("a51w5");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_a51w5[i])+")", tableTrace0.contains(Arrays.toString(records_a51w5[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test117TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"qqp","x1856","pjx","l27","d0941","d69","of9c","dz6"};
		DBApp.createTable("y9gy3", cols0);
		String [][] records_y9gy3 = new String[6][cols0.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_y9gy3[i][j] = genRandString();
			}
			DBApp.insert("y9gy3", records_y9gy3[i]);
			String tableTrace0 = DBApp.getLastTrace("y9gy3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_y9gy3[i])+")", tableTrace0.contains(Arrays.toString(records_y9gy3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"f41a1","h1o","u1g1","wn5","x7","mnz","dcgp","kig5","fp9","xudm1"};
		DBApp.createTable("vr7m", cols1);
		String [][] records_vr7m = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_vr7m[i][j] = genRandString();
			}
			DBApp.insert("vr7m", records_vr7m[i]);
			String tableTrace1 = DBApp.getLastTrace("vr7m");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_vr7m[i])+")", tableTrace1.contains(Arrays.toString(records_vr7m[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"ck","f221","oobn","l72x8","p23","wdr","d9"};
		DBApp.createTable("e0", cols2);
		String [][] records_e0 = new String[49][cols2.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_e0[i][j] = genRandString();
			}
			DBApp.insert("e0", records_e0[i]);
			String tableTrace2 = DBApp.getLastTrace("e0");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_e0[i])+")", tableTrace2.contains(Arrays.toString(records_e0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"c30","bmv7","ky","kjp8"};
		DBApp.createTable("d9l", cols3);
		String [][] records_d9l = new String[98][cols3.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_d9l[i][j] = genRandString();
			}
			DBApp.insert("d9l", records_d9l[i]);
			String tableTrace3 = DBApp.getLastTrace("d9l");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_d9l[i])+")", tableTrace3.contains(Arrays.toString(records_d9l[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"qi0","m4w71"};
		DBApp.createTable("bex", cols4);
		String [][] records_bex = new String[24][cols4.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_bex[i][j] = genRandString();
			}
			DBApp.insert("bex", records_bex[i]);
			String tableTrace4 = DBApp.getLastTrace("bex");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_bex[i])+")", tableTrace4.contains(Arrays.toString(records_bex[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test118TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"kfkz"};
		DBApp.createTable("ye", cols0);
		String [][] records_ye = new String[96][cols0.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ye[i][j] = genRandString();
			}
			DBApp.insert("ye", records_ye[i]);
			String tableTrace0 = DBApp.getLastTrace("ye");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ye[i])+")", tableTrace0.contains(Arrays.toString(records_ye[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"s7r","z2j","t0ctb"};
		DBApp.createTable("v835", cols1);
		String [][] records_v835 = new String[20][cols1.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_v835[i][j] = genRandString();
			}
			DBApp.insert("v835", records_v835[i]);
			String tableTrace1 = DBApp.getLastTrace("v835");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_v835[i])+")", tableTrace1.contains(Arrays.toString(records_v835[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"rp2i","df8s4","xwkmf","pob6","e5","ab","cw7t"};
		DBApp.createTable("x8", cols2);
		String [][] records_x8 = new String[18][cols2.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_x8[i][j] = genRandString();
			}
			DBApp.insert("x8", records_x8[i]);
			String tableTrace2 = DBApp.getLastTrace("x8");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_x8[i])+")", tableTrace2.contains(Arrays.toString(records_x8[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"s3n63","w0x","bu71t","v00h","hv29"};
		DBApp.createTable("fn9uo", cols3);
		String [][] records_fn9uo = new String[53][cols3.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_fn9uo[i][j] = genRandString();
			}
			DBApp.insert("fn9uo", records_fn9uo[i]);
			String tableTrace3 = DBApp.getLastTrace("fn9uo");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_fn9uo[i])+")", tableTrace3.contains(Arrays.toString(records_fn9uo[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"wm","y34","b6p"};
		DBApp.createTable("f35", cols4);
		String [][] records_f35 = new String[74][cols4.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_f35[i][j] = genRandString();
			}
			DBApp.insert("f35", records_f35[i]);
			String tableTrace4 = DBApp.getLastTrace("f35");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_f35[i])+")", tableTrace4.contains(Arrays.toString(records_f35[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test119TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"b880","yd6","pt0y","l8p","dpu7","op","vrxe"};
		DBApp.createTable("n08", cols0);
		String [][] records_n08 = new String[53][cols0.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_n08[i][j] = genRandString();
			}
			DBApp.insert("n08", records_n08[i]);
			String tableTrace0 = DBApp.getLastTrace("n08");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_n08[i])+")", tableTrace0.contains(Arrays.toString(records_n08[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"m6o06","ri"};
		DBApp.createTable("vlc", cols1);
		String [][] records_vlc = new String[32][cols1.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_vlc[i][j] = genRandString();
			}
			DBApp.insert("vlc", records_vlc[i]);
			String tableTrace1 = DBApp.getLastTrace("vlc");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_vlc[i])+")", tableTrace1.contains(Arrays.toString(records_vlc[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test120TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"yq","z5","v0m","xbu"};
		DBApp.createTable("a8sy", cols0);
		String [][] records_a8sy = new String[86][cols0.length];
		for(int i=0;i<86;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_a8sy[i][j] = genRandString();
			}
			DBApp.insert("a8sy", records_a8sy[i]);
			String tableTrace0 = DBApp.getLastTrace("a8sy");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_a8sy[i])+")", tableTrace0.contains(Arrays.toString(records_a8sy[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"r7fa6"};
		DBApp.createTable("id3", cols1);
		String [][] records_id3 = new String[71][cols1.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_id3[i][j] = genRandString();
			}
			DBApp.insert("id3", records_id3[i]);
			String tableTrace1 = DBApp.getLastTrace("id3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_id3[i])+")", tableTrace1.contains(Arrays.toString(records_id3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a2v","j84","w42","y4l","d330t"};
		DBApp.createTable("j9085", cols2);
		String [][] records_j9085 = new String[46][cols2.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_j9085[i][j] = genRandString();
			}
			DBApp.insert("j9085", records_j9085[i]);
			String tableTrace2 = DBApp.getLastTrace("j9085");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_j9085[i])+")", tableTrace2.contains(Arrays.toString(records_j9085[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test121TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"qd","s5z","c07","n1","any","x9sl","u05x","al06"};
		DBApp.createTable("i38", cols0);
		String [][] records_i38 = new String[13][cols0.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i38[i][j] = genRandString();
			}
			DBApp.insert("i38", records_i38[i]);
			String tableTrace0 = DBApp.getLastTrace("i38");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_i38[i])+")", tableTrace0.contains(Arrays.toString(records_i38[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"nrn0","lplu5"};
		DBApp.createTable("c78", cols1);
		String [][] records_c78 = new String[51][cols1.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_c78[i][j] = genRandString();
			}
			DBApp.insert("c78", records_c78[i]);
			String tableTrace1 = DBApp.getLastTrace("c78");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_c78[i])+")", tableTrace1.contains(Arrays.toString(records_c78[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"nvo8b","zjt8","qx8","my37","v93mk","v9o","wdu05","ph3z4"};
		DBApp.createTable("ip08", cols2);
		String [][] records_ip08 = new String[94][cols2.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ip08[i][j] = genRandString();
			}
			DBApp.insert("ip08", records_ip08[i]);
			String tableTrace2 = DBApp.getLastTrace("ip08");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ip08[i])+")", tableTrace2.contains(Arrays.toString(records_ip08[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test122TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"zvo","y2k","q5g","z54","dl","adoc"};
		DBApp.createTable("pl2", cols0);
		String [][] records_pl2 = new String[80][cols0.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_pl2[i][j] = genRandString();
			}
			DBApp.insert("pl2", records_pl2[i]);
			String tableTrace0 = DBApp.getLastTrace("pl2");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_pl2[i])+")", tableTrace0.contains(Arrays.toString(records_pl2[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test123TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"yp83","m331v","y3","gj7","e20","un2"};
		DBApp.createTable("tn", cols0);
		String [][] records_tn = new String[62][cols0.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_tn[i][j] = genRandString();
			}
			DBApp.insert("tn", records_tn[i]);
			String tableTrace0 = DBApp.getLastTrace("tn");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_tn[i])+")", tableTrace0.contains(Arrays.toString(records_tn[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"kj","n4","m6","p7f","f10","p12","wd1","mwz","wmod"};
		DBApp.createTable("w9", cols1);
		String [][] records_w9 = new String[96][cols1.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_w9[i][j] = genRandString();
			}
			DBApp.insert("w9", records_w9[i]);
			String tableTrace1 = DBApp.getLastTrace("w9");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_w9[i])+")", tableTrace1.contains(Arrays.toString(records_w9[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"d51","d011","veh","vl6bt","m4","iij8u","hm2","pw","m98n8","s13pv"};
		DBApp.createTable("e2r", cols2);
		String [][] records_e2r = new String[60][cols2.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_e2r[i][j] = genRandString();
			}
			DBApp.insert("e2r", records_e2r[i]);
			String tableTrace2 = DBApp.getLastTrace("e2r");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_e2r[i])+")", tableTrace2.contains(Arrays.toString(records_e2r[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"ah5c","q3o6","j7","gn4t","rf1","rt6","nku","b8ok","e6x","u9e"};
		DBApp.createTable("n8y", cols3);
		String [][] records_n8y = new String[20][cols3.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_n8y[i][j] = genRandString();
			}
			DBApp.insert("n8y", records_n8y[i]);
			String tableTrace3 = DBApp.getLastTrace("n8y");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_n8y[i])+")", tableTrace3.contains(Arrays.toString(records_n8y[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"is3","t2","tnso","d5","p768","y7k","uw1","bzs","jwy","x9605"};
		DBApp.createTable("i3j", cols4);
		String [][] records_i3j = new String[4][cols4.length];
		for(int i=0;i<4;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_i3j[i][j] = genRandString();
			}
			DBApp.insert("i3j", records_i3j[i]);
			String tableTrace4 = DBApp.getLastTrace("i3j");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_i3j[i])+")", tableTrace4.contains(Arrays.toString(records_i3j[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test124TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"sd","jp","d24","uqfe6","r213"};
		DBApp.createTable("nb95q", cols0);
		String [][] records_nb95q = new String[61][cols0.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_nb95q[i][j] = genRandString();
			}
			DBApp.insert("nb95q", records_nb95q[i]);
			String tableTrace0 = DBApp.getLastTrace("nb95q");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_nb95q[i])+")", tableTrace0.contains(Arrays.toString(records_nb95q[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"ig785","atf3","w64f9","t2i","yy4","s43","j04","at4","aq6","cx"};
		DBApp.createTable("pr4p", cols1);
		String [][] records_pr4p = new String[61][cols1.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_pr4p[i][j] = genRandString();
			}
			DBApp.insert("pr4p", records_pr4p[i]);
			String tableTrace1 = DBApp.getLastTrace("pr4p");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_pr4p[i])+")", tableTrace1.contains(Arrays.toString(records_pr4p[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"z253i","tt6","bwz5","p62","b3","g1","cx3","l47","k54","n3"};
		DBApp.createTable("ly7p0", cols2);
		String [][] records_ly7p0 = new String[78][cols2.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ly7p0[i][j] = genRandString();
			}
			DBApp.insert("ly7p0", records_ly7p0[i]);
			String tableTrace2 = DBApp.getLastTrace("ly7p0");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ly7p0[i])+")", tableTrace2.contains(Arrays.toString(records_ly7p0[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test125TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"lm8d","d77c7","vyv","uip","z4cb0","jhl7","r9a41","f9e","ny2","ul"};
		DBApp.createTable("ake", cols0);
		String [][] records_ake = new String[86][cols0.length];
		for(int i=0;i<86;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ake[i][j] = genRandString();
			}
			DBApp.insert("ake", records_ake[i]);
			String tableTrace0 = DBApp.getLastTrace("ake");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ake[i])+")", tableTrace0.contains(Arrays.toString(records_ake[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test126TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"u4","vue","mm5","m3","d3zu","w3k1v","j76","h3986","a3o80","ijv"};
		DBApp.createTable("j7y83", cols0);
		String [][] records_j7y83 = new String[54][cols0.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_j7y83[i][j] = genRandString();
			}
			DBApp.insert("j7y83", records_j7y83[i]);
			String tableTrace0 = DBApp.getLastTrace("j7y83");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_j7y83[i])+")", tableTrace0.contains(Arrays.toString(records_j7y83[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"l0wj5"};
		DBApp.createTable("gb2w", cols1);
		String [][] records_gb2w = new String[46][cols1.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_gb2w[i][j] = genRandString();
			}
			DBApp.insert("gb2w", records_gb2w[i]);
			String tableTrace1 = DBApp.getLastTrace("gb2w");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_gb2w[i])+")", tableTrace1.contains(Arrays.toString(records_gb2w[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"au18","jz4r","u97","c7o","fq","c1v6","i4e","o97k3","uh3"};
		DBApp.createTable("hwo", cols2);
		String [][] records_hwo = new String[26][cols2.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_hwo[i][j] = genRandString();
			}
			DBApp.insert("hwo", records_hwo[i]);
			String tableTrace2 = DBApp.getLastTrace("hwo");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_hwo[i])+")", tableTrace2.contains(Arrays.toString(records_hwo[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test127TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"e5","x4","c2pe","xh","p7vjo","i9e"};
		DBApp.createTable("gn", cols0);
		String [][] records_gn = new String[67][cols0.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_gn[i][j] = genRandString();
			}
			DBApp.insert("gn", records_gn[i]);
			String tableTrace0 = DBApp.getLastTrace("gn");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_gn[i])+")", tableTrace0.contains(Arrays.toString(records_gn[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"o63","q4d63","st6r","j48u","a2w","zy4","g4"};
		DBApp.createTable("gc1", cols1);
		String [][] records_gc1 = new String[71][cols1.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_gc1[i][j] = genRandString();
			}
			DBApp.insert("gc1", records_gc1[i]);
			String tableTrace1 = DBApp.getLastTrace("gc1");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_gc1[i])+")", tableTrace1.contains(Arrays.toString(records_gc1[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"y3","nm01","y3c","e26"};
		DBApp.createTable("d507", cols2);
		String [][] records_d507 = new String[91][cols2.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_d507[i][j] = genRandString();
			}
			DBApp.insert("d507", records_d507[i]);
			String tableTrace2 = DBApp.getLastTrace("d507");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_d507[i])+")", tableTrace2.contains(Arrays.toString(records_d507[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"o47","z28","j4","m4","vr101","hotyj","m3"};
		DBApp.createTable("dtjo", cols3);
		String [][] records_dtjo = new String[97][cols3.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_dtjo[i][j] = genRandString();
			}
			DBApp.insert("dtjo", records_dtjo[i]);
			String tableTrace3 = DBApp.getLastTrace("dtjo");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_dtjo[i])+")", tableTrace3.contains(Arrays.toString(records_dtjo[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"vtg","ff94x","m6e2","j8ss"};
		DBApp.createTable("i1z", cols4);
		String [][] records_i1z = new String[98][cols4.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_i1z[i][j] = genRandString();
			}
			DBApp.insert("i1z", records_i1z[i]);
			String tableTrace4 = DBApp.getLastTrace("i1z");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_i1z[i])+")", tableTrace4.contains(Arrays.toString(records_i1z[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test128TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"e61","pfi","v5","v2ba","o6","mu"};
		DBApp.createTable("o92", cols0);
		String [][] records_o92 = new String[65][cols0.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o92[i][j] = genRandString();
			}
			DBApp.insert("o92", records_o92[i]);
			String tableTrace0 = DBApp.getLastTrace("o92");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_o92[i])+")", tableTrace0.contains(Arrays.toString(records_o92[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"s8","nf22h"};
		DBApp.createTable("n7", cols1);
		String [][] records_n7 = new String[14][cols1.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_n7[i][j] = genRandString();
			}
			DBApp.insert("n7", records_n7[i]);
			String tableTrace1 = DBApp.getLastTrace("n7");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_n7[i])+")", tableTrace1.contains(Arrays.toString(records_n7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"c1gn","ch98","k2x38","g7d6","t70","v24vk","jd","dlqu"};
		DBApp.createTable("b0q", cols2);
		String [][] records_b0q = new String[58][cols2.length];
		for(int i=0;i<58;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_b0q[i][j] = genRandString();
			}
			DBApp.insert("b0q", records_b0q[i]);
			String tableTrace2 = DBApp.getLastTrace("b0q");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_b0q[i])+")", tableTrace2.contains(Arrays.toString(records_b0q[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test129TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"n4b","f7imo","li9"};
		DBApp.createTable("o5", cols0);
		String [][] records_o5 = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o5[i][j] = genRandString();
			}
			DBApp.insert("o5", records_o5[i]);
			String tableTrace0 = DBApp.getLastTrace("o5");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_o5[i])+")", tableTrace0.contains(Arrays.toString(records_o5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"x73i","ub5j1","v405","d004","ge","a50i"};
		DBApp.createTable("ti04", cols1);
		String [][] records_ti04 = new String[19][cols1.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ti04[i][j] = genRandString();
			}
			DBApp.insert("ti04", records_ti04[i]);
			String tableTrace1 = DBApp.getLastTrace("ti04");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ti04[i])+")", tableTrace1.contains(Arrays.toString(records_ti04[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"wik","pqq","sg4mm","g8u","bp2hi","ials2","vc","uqn","mz6","j0b8"};
		DBApp.createTable("uk4", cols2);
		String [][] records_uk4 = new String[88][cols2.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_uk4[i][j] = genRandString();
			}
			DBApp.insert("uk4", records_uk4[i]);
			String tableTrace2 = DBApp.getLastTrace("uk4");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_uk4[i])+")", tableTrace2.contains(Arrays.toString(records_uk4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"w5z","q9","qh4","l21a"};
		DBApp.createTable("z685", cols3);
		String [][] records_z685 = new String[5][cols3.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_z685[i][j] = genRandString();
			}
			DBApp.insert("z685", records_z685[i]);
			String tableTrace3 = DBApp.getLastTrace("z685");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_z685[i])+")", tableTrace3.contains(Arrays.toString(records_z685[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"m2"};
		DBApp.createTable("farz1", cols4);
		String [][] records_farz1 = new String[27][cols4.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_farz1[i][j] = genRandString();
			}
			DBApp.insert("farz1", records_farz1[i]);
			String tableTrace4 = DBApp.getLastTrace("farz1");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_farz1[i])+")", tableTrace4.contains(Arrays.toString(records_farz1[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test130TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"dt5","p68","u17","y0nla"};
		DBApp.createTable("q8", cols0);
		String [][] records_q8 = new String[9][cols0.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_q8[i][j] = genRandString();
			}
			DBApp.insert("q8", records_q8[i]);
			String tableTrace0 = DBApp.getLastTrace("q8");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_q8[i])+")", tableTrace0.contains(Arrays.toString(records_q8[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"p2e","hgv","hkq","rfa","of","l265","p4","lca4","c0a","u50l"};
		DBApp.createTable("g719z", cols1);
		String [][] records_g719z = new String[26][cols1.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_g719z[i][j] = genRandString();
			}
			DBApp.insert("g719z", records_g719z[i]);
			String tableTrace1 = DBApp.getLastTrace("g719z");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_g719z[i])+")", tableTrace1.contains(Arrays.toString(records_g719z[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test131TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"oo","m5i04","l1s","ho5","g0v","wa4y","e34n"};
		DBApp.createTable("v1", cols0);
		String [][] records_v1 = new String[70][cols0.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_v1[i][j] = genRandString();
			}
			DBApp.insert("v1", records_v1[i]);
			String tableTrace0 = DBApp.getLastTrace("v1");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_v1[i])+")", tableTrace0.contains(Arrays.toString(records_v1[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"dra","vul","g7b","jla","i9"};
		DBApp.createTable("z7b0", cols1);
		String [][] records_z7b0 = new String[17][cols1.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_z7b0[i][j] = genRandString();
			}
			DBApp.insert("z7b0", records_z7b0[i]);
			String tableTrace1 = DBApp.getLastTrace("z7b0");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_z7b0[i])+")", tableTrace1.contains(Arrays.toString(records_z7b0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"d711e","vb","inc","v3b83","tl9","b085","ep2"};
		DBApp.createTable("ak6o", cols2);
		String [][] records_ak6o = new String[91][cols2.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ak6o[i][j] = genRandString();
			}
			DBApp.insert("ak6o", records_ak6o[i]);
			String tableTrace2 = DBApp.getLastTrace("ak6o");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ak6o[i])+")", tableTrace2.contains(Arrays.toString(records_ak6o[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"avns","iup","le","gx2n","todse"};
		DBApp.createTable("op0", cols3);
		String [][] records_op0 = new String[2][cols3.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_op0[i][j] = genRandString();
			}
			DBApp.insert("op0", records_op0[i]);
			String tableTrace3 = DBApp.getLastTrace("op0");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_op0[i])+")", tableTrace3.contains(Arrays.toString(records_op0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"rgr8","q6pc1"};
		DBApp.createTable("c7222", cols4);
		String [][] records_c7222 = new String[15][cols4.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_c7222[i][j] = genRandString();
			}
			DBApp.insert("c7222", records_c7222[i]);
			String tableTrace4 = DBApp.getLastTrace("c7222");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_c7222[i])+")", tableTrace4.contains(Arrays.toString(records_c7222[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test132TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"x2h8w","m4be","r7","q9w9f","n06","ux","t7i","g97aa"};
		DBApp.createTable("ux1", cols0);
		String [][] records_ux1 = new String[43][cols0.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ux1[i][j] = genRandString();
			}
			DBApp.insert("ux1", records_ux1[i]);
			String tableTrace0 = DBApp.getLastTrace("ux1");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ux1[i])+")", tableTrace0.contains(Arrays.toString(records_ux1[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"vs0","i57"};
		DBApp.createTable("r2bdx", cols1);
		String [][] records_r2bdx = new String[21][cols1.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_r2bdx[i][j] = genRandString();
			}
			DBApp.insert("r2bdx", records_r2bdx[i]);
			String tableTrace1 = DBApp.getLastTrace("r2bdx");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_r2bdx[i])+")", tableTrace1.contains(Arrays.toString(records_r2bdx[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test133TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"v4n5","ssjz4"};
		DBApp.createTable("i3l", cols0);
		String [][] records_i3l = new String[2][cols0.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i3l[i][j] = genRandString();
			}
			DBApp.insert("i3l", records_i3l[i]);
			String tableTrace0 = DBApp.getLastTrace("i3l");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_i3l[i])+")", tableTrace0.contains(Arrays.toString(records_i3l[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test134TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"az95","nj","j34","sp","ijq0","o7s1","qv9k","vjc3"};
		DBApp.createTable("w3f", cols0);
		String [][] records_w3f = new String[11][cols0.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_w3f[i][j] = genRandString();
			}
			DBApp.insert("w3f", records_w3f[i]);
			String tableTrace0 = DBApp.getLastTrace("w3f");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_w3f[i])+")", tableTrace0.contains(Arrays.toString(records_w3f[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"agkz","cq","y2wx","hta","mr8"};
		DBApp.createTable("ipv3", cols1);
		String [][] records_ipv3 = new String[36][cols1.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ipv3[i][j] = genRandString();
			}
			DBApp.insert("ipv3", records_ipv3[i]);
			String tableTrace1 = DBApp.getLastTrace("ipv3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ipv3[i])+")", tableTrace1.contains(Arrays.toString(records_ipv3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"r6t","m46z8","y404e","c2","k4s","ypoy4","o1w"};
		DBApp.createTable("ezbn", cols2);
		String [][] records_ezbn = new String[73][cols2.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ezbn[i][j] = genRandString();
			}
			DBApp.insert("ezbn", records_ezbn[i]);
			String tableTrace2 = DBApp.getLastTrace("ezbn");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ezbn[i])+")", tableTrace2.contains(Arrays.toString(records_ezbn[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test135TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"sngyt"};
		DBApp.createTable("m2u", cols0);
		String [][] records_m2u = new String[81][cols0.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_m2u[i][j] = genRandString();
			}
			DBApp.insert("m2u", records_m2u[i]);
			String tableTrace0 = DBApp.getLastTrace("m2u");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_m2u[i])+")", tableTrace0.contains(Arrays.toString(records_m2u[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"bhs8","yz1","t5v5","p5r73","ri","w6","jjm0"};
		DBApp.createTable("eep", cols1);
		String [][] records_eep = new String[96][cols1.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_eep[i][j] = genRandString();
			}
			DBApp.insert("eep", records_eep[i]);
			String tableTrace1 = DBApp.getLastTrace("eep");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_eep[i])+")", tableTrace1.contains(Arrays.toString(records_eep[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"rr65"};
		DBApp.createTable("mu23", cols2);
		String [][] records_mu23 = new String[79][cols2.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_mu23[i][j] = genRandString();
			}
			DBApp.insert("mu23", records_mu23[i]);
			String tableTrace2 = DBApp.getLastTrace("mu23");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_mu23[i])+")", tableTrace2.contains(Arrays.toString(records_mu23[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test136TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"p7y","uy056","ee6","rj22c","fys3f","w8e","w79","e7f"};
		DBApp.createTable("vh3", cols0);
		String [][] records_vh3 = new String[41][cols0.length];
		for(int i=0;i<41;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vh3[i][j] = genRandString();
			}
			DBApp.insert("vh3", records_vh3[i]);
			String tableTrace0 = DBApp.getLastTrace("vh3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_vh3[i])+")", tableTrace0.contains(Arrays.toString(records_vh3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"vobh4","ic1"};
		DBApp.createTable("zr4", cols1);
		String [][] records_zr4 = new String[79][cols1.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_zr4[i][j] = genRandString();
			}
			DBApp.insert("zr4", records_zr4[i]);
			String tableTrace1 = DBApp.getLastTrace("zr4");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_zr4[i])+")", tableTrace1.contains(Arrays.toString(records_zr4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"ad","vr","r66","g8","t1"};
		DBApp.createTable("z9f73", cols2);
		String [][] records_z9f73 = new String[94][cols2.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_z9f73[i][j] = genRandString();
			}
			DBApp.insert("z9f73", records_z9f73[i]);
			String tableTrace2 = DBApp.getLastTrace("z9f73");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_z9f73[i])+")", tableTrace2.contains(Arrays.toString(records_z9f73[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"oic5","x5","l88","vbqo","mf4xt","i3zg","qc7","ws","n9vhz"};
		DBApp.createTable("v6", cols3);
		String [][] records_v6 = new String[9][cols3.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_v6[i][j] = genRandString();
			}
			DBApp.insert("v6", records_v6[i]);
			String tableTrace3 = DBApp.getLastTrace("v6");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_v6[i])+")", tableTrace3.contains(Arrays.toString(records_v6[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test137TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"ya6j2","zu1","l98qx","qe","d6","jh48o"};
		DBApp.createTable("a4x1", cols0);
		String [][] records_a4x1 = new String[47][cols0.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_a4x1[i][j] = genRandString();
			}
			DBApp.insert("a4x1", records_a4x1[i]);
			String tableTrace0 = DBApp.getLastTrace("a4x1");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_a4x1[i])+")", tableTrace0.contains(Arrays.toString(records_a4x1[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"fn0k"};
		DBApp.createTable("s91", cols1);
		String [][] records_s91 = new String[88][cols1.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_s91[i][j] = genRandString();
			}
			DBApp.insert("s91", records_s91[i]);
			String tableTrace1 = DBApp.getLastTrace("s91");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_s91[i])+")", tableTrace1.contains(Arrays.toString(records_s91[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"w1093","k867"};
		DBApp.createTable("ya", cols2);
		String [][] records_ya = new String[76][cols2.length];
		for(int i=0;i<76;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ya[i][j] = genRandString();
			}
			DBApp.insert("ya", records_ya[i]);
			String tableTrace2 = DBApp.getLastTrace("ya");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ya[i])+")", tableTrace2.contains(Arrays.toString(records_ya[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test138TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"gew","q5b","c9v","js3ln","j3jp"};
		DBApp.createTable("r1w", cols0);
		String [][] records_r1w = new String[52][cols0.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r1w[i][j] = genRandString();
			}
			DBApp.insert("r1w", records_r1w[i]);
			String tableTrace0 = DBApp.getLastTrace("r1w");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_r1w[i])+")", tableTrace0.contains(Arrays.toString(records_r1w[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test139TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"v23","z47v","m7vn1","v5"};
		DBApp.createTable("p4o9", cols0);
		String [][] records_p4o9 = new String[65][cols0.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p4o9[i][j] = genRandString();
			}
			DBApp.insert("p4o9", records_p4o9[i]);
			String tableTrace0 = DBApp.getLastTrace("p4o9");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_p4o9[i])+")", tableTrace0.contains(Arrays.toString(records_p4o9[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"m2","o77bj","xln","c6o","r1p","j0","yt0l"};
		DBApp.createTable("k5", cols1);
		String [][] records_k5 = new String[42][cols1.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_k5[i][j] = genRandString();
			}
			DBApp.insert("k5", records_k5[i]);
			String tableTrace1 = DBApp.getLastTrace("k5");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_k5[i])+")", tableTrace1.contains(Arrays.toString(records_k5[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test140TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"o0rj","g069t"};
		DBApp.createTable("g59", cols0);
		String [][] records_g59 = new String[67][cols0.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_g59[i][j] = genRandString();
			}
			DBApp.insert("g59", records_g59[i]);
			String tableTrace0 = DBApp.getLastTrace("g59");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_g59[i])+")", tableTrace0.contains(Arrays.toString(records_g59[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"qg27","ps5","ediry","k239q","u0s8","sqc"};
		DBApp.createTable("v4srw", cols1);
		String [][] records_v4srw = new String[85][cols1.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_v4srw[i][j] = genRandString();
			}
			DBApp.insert("v4srw", records_v4srw[i]);
			String tableTrace1 = DBApp.getLastTrace("v4srw");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_v4srw[i])+")", tableTrace1.contains(Arrays.toString(records_v4srw[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test141TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"muue","x7s","thx50","wzr5","q3","cq"};
		DBApp.createTable("qw5", cols0);
		String [][] records_qw5 = new String[84][cols0.length];
		for(int i=0;i<84;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_qw5[i][j] = genRandString();
			}
			DBApp.insert("qw5", records_qw5[i]);
			String tableTrace0 = DBApp.getLastTrace("qw5");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_qw5[i])+")", tableTrace0.contains(Arrays.toString(records_qw5[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test142TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"e36","q0n1","slh","qux","l39","ym6j","l0","i0y","a06"};
		DBApp.createTable("f707", cols0);
		String [][] records_f707 = new String[96][cols0.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_f707[i][j] = genRandString();
			}
			DBApp.insert("f707", records_f707[i]);
			String tableTrace0 = DBApp.getLastTrace("f707");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_f707[i])+")", tableTrace0.contains(Arrays.toString(records_f707[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"e3o","ez"};
		DBApp.createTable("b7p3o", cols1);
		String [][] records_b7p3o = new String[66][cols1.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_b7p3o[i][j] = genRandString();
			}
			DBApp.insert("b7p3o", records_b7p3o[i]);
			String tableTrace1 = DBApp.getLastTrace("b7p3o");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_b7p3o[i])+")", tableTrace1.contains(Arrays.toString(records_b7p3o[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"bj0jz","j6","pj","bvk07","tt6","o6h9"};
		DBApp.createTable("h5u3", cols2);
		String [][] records_h5u3 = new String[11][cols2.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_h5u3[i][j] = genRandString();
			}
			DBApp.insert("h5u3", records_h5u3[i]);
			String tableTrace2 = DBApp.getLastTrace("h5u3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_h5u3[i])+")", tableTrace2.contains(Arrays.toString(records_h5u3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"qa2","x02"};
		DBApp.createTable("j75", cols3);
		String [][] records_j75 = new String[93][cols3.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_j75[i][j] = genRandString();
			}
			DBApp.insert("j75", records_j75[i]);
			String tableTrace3 = DBApp.getLastTrace("j75");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_j75[i])+")", tableTrace3.contains(Arrays.toString(records_j75[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test143TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"q4","p0","ogt7","u6","v65"};
		DBApp.createTable("co3w", cols0);
		String [][] records_co3w = new String[6][cols0.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_co3w[i][j] = genRandString();
			}
			DBApp.insert("co3w", records_co3w[i]);
			String tableTrace0 = DBApp.getLastTrace("co3w");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_co3w[i])+")", tableTrace0.contains(Arrays.toString(records_co3w[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"xg78y","vc5","m54"};
		DBApp.createTable("vh", cols1);
		String [][] records_vh = new String[19][cols1.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_vh[i][j] = genRandString();
			}
			DBApp.insert("vh", records_vh[i]);
			String tableTrace1 = DBApp.getLastTrace("vh");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_vh[i])+")", tableTrace1.contains(Arrays.toString(records_vh[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test144TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"n6m8e","z1","p56","m69","iv","vv7","y4","bi8e5","vb","x73"};
		DBApp.createTable("gf", cols0);
		String [][] records_gf = new String[16][cols0.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_gf[i][j] = genRandString();
			}
			DBApp.insert("gf", records_gf[i]);
			String tableTrace0 = DBApp.getLastTrace("gf");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_gf[i])+")", tableTrace0.contains(Arrays.toString(records_gf[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test145TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"nc"};
		DBApp.createTable("hb", cols0);
		String [][] records_hb = new String[12][cols0.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_hb[i][j] = genRandString();
			}
			DBApp.insert("hb", records_hb[i]);
			String tableTrace0 = DBApp.getLastTrace("hb");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_hb[i])+")", tableTrace0.contains(Arrays.toString(records_hb[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"uw1","oz","o9","kh9ln","xo6","b7","eq2y","xmwox"};
		DBApp.createTable("ggb", cols1);
		String [][] records_ggb = new String[62][cols1.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ggb[i][j] = genRandString();
			}
			DBApp.insert("ggb", records_ggb[i]);
			String tableTrace1 = DBApp.getLastTrace("ggb");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ggb[i])+")", tableTrace1.contains(Arrays.toString(records_ggb[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"r5u","m36"};
		DBApp.createTable("smam9", cols2);
		String [][] records_smam9 = new String[95][cols2.length];
		for(int i=0;i<95;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_smam9[i][j] = genRandString();
			}
			DBApp.insert("smam9", records_smam9[i]);
			String tableTrace2 = DBApp.getLastTrace("smam9");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_smam9[i])+")", tableTrace2.contains(Arrays.toString(records_smam9[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"ruf","j9f","t45","t5","d93","c3q"};
		DBApp.createTable("m53", cols3);
		String [][] records_m53 = new String[79][cols3.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_m53[i][j] = genRandString();
			}
			DBApp.insert("m53", records_m53[i]);
			String tableTrace3 = DBApp.getLastTrace("m53");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_m53[i])+")", tableTrace3.contains(Arrays.toString(records_m53[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"f7r","oc7","cg631","eeq","m6h8","u63","o65"};
		DBApp.createTable("xiq", cols4);
		String [][] records_xiq = new String[8][cols4.length];
		for(int i=0;i<8;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_xiq[i][j] = genRandString();
			}
			DBApp.insert("xiq", records_xiq[i]);
			String tableTrace4 = DBApp.getLastTrace("xiq");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_xiq[i])+")", tableTrace4.contains(Arrays.toString(records_xiq[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test146TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a4"};
		DBApp.createTable("u8", cols0);
		String [][] records_u8 = new String[93][cols0.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u8[i][j] = genRandString();
			}
			DBApp.insert("u8", records_u8[i]);
			String tableTrace0 = DBApp.getLastTrace("u8");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_u8[i])+")", tableTrace0.contains(Arrays.toString(records_u8[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"z3","grz","yx3p","yjmmk","l6","b132"};
		DBApp.createTable("mld0", cols1);
		String [][] records_mld0 = new String[22][cols1.length];
		for(int i=0;i<22;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_mld0[i][j] = genRandString();
			}
			DBApp.insert("mld0", records_mld0[i]);
			String tableTrace1 = DBApp.getLastTrace("mld0");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_mld0[i])+")", tableTrace1.contains(Arrays.toString(records_mld0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"zanj2","o6t","vpgd1","jw4","c124","od0","m3hco","lw6","inz"};
		DBApp.createTable("q3n", cols2);
		String [][] records_q3n = new String[9][cols2.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_q3n[i][j] = genRandString();
			}
			DBApp.insert("q3n", records_q3n[i]);
			String tableTrace2 = DBApp.getLastTrace("q3n");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_q3n[i])+")", tableTrace2.contains(Arrays.toString(records_q3n[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"r2s0","z20qv","jr1","j7ale","f94","w4u","l4k"};
		DBApp.createTable("t3e", cols3);
		String [][] records_t3e = new String[43][cols3.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_t3e[i][j] = genRandString();
			}
			DBApp.insert("t3e", records_t3e[i]);
			String tableTrace3 = DBApp.getLastTrace("t3e");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_t3e[i])+")", tableTrace3.contains(Arrays.toString(records_t3e[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"d3","s8l","kttn7","wl39","b97a","v7b","r23","e2","ckkrn"};
		DBApp.createTable("bkjx", cols4);
		String [][] records_bkjx = new String[17][cols4.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_bkjx[i][j] = genRandString();
			}
			DBApp.insert("bkjx", records_bkjx[i]);
			String tableTrace4 = DBApp.getLastTrace("bkjx");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_bkjx[i])+")", tableTrace4.contains(Arrays.toString(records_bkjx[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test147TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"j8tw9","p5","iys5k","c5678","ad","gge","u34","n70z","t4is0"};
		DBApp.createTable("zg", cols0);
		String [][] records_zg = new String[91][cols0.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_zg[i][j] = genRandString();
			}
			DBApp.insert("zg", records_zg[i]);
			String tableTrace0 = DBApp.getLastTrace("zg");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_zg[i])+")", tableTrace0.contains(Arrays.toString(records_zg[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"f39","kvj","c64i","n9j","az343"};
		DBApp.createTable("mj993", cols1);
		String [][] records_mj993 = new String[98][cols1.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_mj993[i][j] = genRandString();
			}
			DBApp.insert("mj993", records_mj993[i]);
			String tableTrace1 = DBApp.getLastTrace("mj993");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_mj993[i])+")", tableTrace1.contains(Arrays.toString(records_mj993[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"zbe","g68","t85","j4","m0i","spp","c4e"};
		DBApp.createTable("pdyk", cols2);
		String [][] records_pdyk = new String[61][cols2.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_pdyk[i][j] = genRandString();
			}
			DBApp.insert("pdyk", records_pdyk[i]);
			String tableTrace2 = DBApp.getLastTrace("pdyk");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_pdyk[i])+")", tableTrace2.contains(Arrays.toString(records_pdyk[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"dw7","fmn3","cp1z0","aar","lv28","aq51s","i08","eu73","wu"};
		DBApp.createTable("x438y", cols3);
		String [][] records_x438y = new String[21][cols3.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_x438y[i][j] = genRandString();
			}
			DBApp.insert("x438y", records_x438y[i]);
			String tableTrace3 = DBApp.getLastTrace("x438y");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_x438y[i])+")", tableTrace3.contains(Arrays.toString(records_x438y[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"dzs","dt","zo5u","j3a3o","et","xa0","y35","rlt","d1p"};
		DBApp.createTable("bngfm", cols4);
		String [][] records_bngfm = new String[34][cols4.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_bngfm[i][j] = genRandString();
			}
			DBApp.insert("bngfm", records_bngfm[i]);
			String tableTrace4 = DBApp.getLastTrace("bngfm");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_bngfm[i])+")", tableTrace4.contains(Arrays.toString(records_bngfm[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test148TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"lf","lc","g0i","zh","o8875","g96h7","vcg","m5t1t","q3","q8h"};
		DBApp.createTable("f6i", cols0);
		String [][] records_f6i = new String[35][cols0.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_f6i[i][j] = genRandString();
			}
			DBApp.insert("f6i", records_f6i[i]);
			String tableTrace0 = DBApp.getLastTrace("f6i");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_f6i[i])+")", tableTrace0.contains(Arrays.toString(records_f6i[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"iuv3","f4"};
		DBApp.createTable("t10", cols1);
		String [][] records_t10 = new String[22][cols1.length];
		for(int i=0;i<22;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_t10[i][j] = genRandString();
			}
			DBApp.insert("t10", records_t10[i]);
			String tableTrace1 = DBApp.getLastTrace("t10");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_t10[i])+")", tableTrace1.contains(Arrays.toString(records_t10[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"u640p","wko","px","rts","y35b","b4q74"};
		DBApp.createTable("f9z", cols2);
		String [][] records_f9z = new String[85][cols2.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_f9z[i][j] = genRandString();
			}
			DBApp.insert("f9z", records_f9z[i]);
			String tableTrace2 = DBApp.getLastTrace("f9z");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_f9z[i])+")", tableTrace2.contains(Arrays.toString(records_f9z[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"p7ahh"};
		DBApp.createTable("llt38", cols3);
		String [][] records_llt38 = new String[39][cols3.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_llt38[i][j] = genRandString();
			}
			DBApp.insert("llt38", records_llt38[i]);
			String tableTrace3 = DBApp.getLastTrace("llt38");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_llt38[i])+")", tableTrace3.contains(Arrays.toString(records_llt38[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"ds3e","qp3o6","kggoz"};
		DBApp.createTable("i23n", cols4);
		String [][] records_i23n = new String[92][cols4.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_i23n[i][j] = genRandString();
			}
			DBApp.insert("i23n", records_i23n[i]);
			String tableTrace4 = DBApp.getLastTrace("i23n");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_i23n[i])+")", tableTrace4.contains(Arrays.toString(records_i23n[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test149TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"ysr","n520a","gi2","k3","q23","z29","k79v0"};
		DBApp.createTable("i39", cols0);
		String [][] records_i39 = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i39[i][j] = genRandString();
			}
			DBApp.insert("i39", records_i39[i]);
			String tableTrace0 = DBApp.getLastTrace("i39");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_i39[i])+")", tableTrace0.contains(Arrays.toString(records_i39[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"t268","ark94","t4g","k4301","siyt","r3629","n3n","o4k"};
		DBApp.createTable("e8", cols1);
		String [][] records_e8 = new String[29][cols1.length];
		for(int i=0;i<29;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_e8[i][j] = genRandString();
			}
			DBApp.insert("e8", records_e8[i]);
			String tableTrace1 = DBApp.getLastTrace("e8");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_e8[i])+")", tableTrace1.contains(Arrays.toString(records_e8[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test150TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"bvj8","b3ft7","bv2k","sm857","o3f","ytw31","vtf"};
		DBApp.createTable("vx0pi", cols0);
		String [][] records_vx0pi = new String[23][cols0.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vx0pi[i][j] = genRandString();
			}
			DBApp.insert("vx0pi", records_vx0pi[i]);
			String tableTrace0 = DBApp.getLastTrace("vx0pi");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_vx0pi[i])+")", tableTrace0.contains(Arrays.toString(records_vx0pi[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test151TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"yp","m7e","m2i","t2nl","k25c","pped"};
		DBApp.createTable("uvcv", cols0);
		String [][] records_uvcv = new String[53][cols0.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_uvcv[i][j] = genRandString();
			}
			DBApp.insert("uvcv", records_uvcv[i]);
			String tableTrace0 = DBApp.getLastTrace("uvcv");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_uvcv[i])+")", tableTrace0.contains(Arrays.toString(records_uvcv[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test152TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"k39q","jza5p","kw28","tnt","b46wu","yi4"};
		DBApp.createTable("tt40c", cols0);
		String [][] records_tt40c = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_tt40c[i][j] = genRandString();
			}
			DBApp.insert("tt40c", records_tt40c[i]);
			String tableTrace0 = DBApp.getLastTrace("tt40c");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_tt40c[i])+")", tableTrace0.contains(Arrays.toString(records_tt40c[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"ixi","g0","p7q"};
		DBApp.createTable("eov", cols1);
		String [][] records_eov = new String[100][cols1.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_eov[i][j] = genRandString();
			}
			DBApp.insert("eov", records_eov[i]);
			String tableTrace1 = DBApp.getLastTrace("eov");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_eov[i])+")", tableTrace1.contains(Arrays.toString(records_eov[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"t6ut6","v6zr","b5g","u5","i6f","b8m"};
		DBApp.createTable("s25b", cols2);
		String [][] records_s25b = new String[1][cols2.length];
		for(int i=0;i<1;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_s25b[i][j] = genRandString();
			}
			DBApp.insert("s25b", records_s25b[i]);
			String tableTrace2 = DBApp.getLastTrace("s25b");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_s25b[i])+")", tableTrace2.contains(Arrays.toString(records_s25b[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"z37z","ku298","yc6y7","g83","z573","edt","cqk"};
		DBApp.createTable("w5", cols3);
		String [][] records_w5 = new String[71][cols3.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_w5[i][j] = genRandString();
			}
			DBApp.insert("w5", records_w5[i]);
			String tableTrace3 = DBApp.getLastTrace("w5");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_w5[i])+")", tableTrace3.contains(Arrays.toString(records_w5[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test153TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"rp6","jo"};
		DBApp.createTable("bk3", cols0);
		String [][] records_bk3 = new String[54][cols0.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_bk3[i][j] = genRandString();
			}
			DBApp.insert("bk3", records_bk3[i]);
			String tableTrace0 = DBApp.getLastTrace("bk3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_bk3[i])+")", tableTrace0.contains(Arrays.toString(records_bk3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"v41et","gol","ys","b6f","bu","v38","o8877","c92vl","qoc"};
		DBApp.createTable("dha06", cols1);
		String [][] records_dha06 = new String[97][cols1.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_dha06[i][j] = genRandString();
			}
			DBApp.insert("dha06", records_dha06[i]);
			String tableTrace1 = DBApp.getLastTrace("dha06");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_dha06[i])+")", tableTrace1.contains(Arrays.toString(records_dha06[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"maa","v6","p13a6","dqu42","m6118","s80","dg99","nrt","u0"};
		DBApp.createTable("w575t", cols2);
		String [][] records_w575t = new String[100][cols2.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_w575t[i][j] = genRandString();
			}
			DBApp.insert("w575t", records_w575t[i]);
			String tableTrace2 = DBApp.getLastTrace("w575t");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_w575t[i])+")", tableTrace2.contains(Arrays.toString(records_w575t[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"f2","l5z","fdno7","y91","czo","swn9","mh0z","wv1","jcx"};
		DBApp.createTable("c14", cols3);
		String [][] records_c14 = new String[97][cols3.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_c14[i][j] = genRandString();
			}
			DBApp.insert("c14", records_c14[i]);
			String tableTrace3 = DBApp.getLastTrace("c14");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_c14[i])+")", tableTrace3.contains(Arrays.toString(records_c14[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test154TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"w7k5"};
		DBApp.createTable("dz4r7", cols0);
		String [][] records_dz4r7 = new String[85][cols0.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_dz4r7[i][j] = genRandString();
			}
			DBApp.insert("dz4r7", records_dz4r7[i]);
			String tableTrace0 = DBApp.getLastTrace("dz4r7");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_dz4r7[i])+")", tableTrace0.contains(Arrays.toString(records_dz4r7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"whs","x0k45","t48","q6ln9","lk","qk","i7v0s","j9q"};
		DBApp.createTable("h4", cols1);
		String [][] records_h4 = new String[77][cols1.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_h4[i][j] = genRandString();
			}
			DBApp.insert("h4", records_h4[i]);
			String tableTrace1 = DBApp.getLastTrace("h4");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_h4[i])+")", tableTrace1.contains(Arrays.toString(records_h4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"ma65","uz338","c449z","ee","bo6p6","iof2","l21","xa8","jbg"};
		DBApp.createTable("o79", cols2);
		String [][] records_o79 = new String[97][cols2.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_o79[i][j] = genRandString();
			}
			DBApp.insert("o79", records_o79[i]);
			String tableTrace2 = DBApp.getLastTrace("o79");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_o79[i])+")", tableTrace2.contains(Arrays.toString(records_o79[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"ovp5","z76v","f7k","dv7","tg6"};
		DBApp.createTable("l02", cols3);
		String [][] records_l02 = new String[43][cols3.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_l02[i][j] = genRandString();
			}
			DBApp.insert("l02", records_l02[i]);
			String tableTrace3 = DBApp.getLastTrace("l02");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_l02[i])+")", tableTrace3.contains(Arrays.toString(records_l02[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test155TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"w969","dxo","i16h","y7q","n7","m3b"};
		DBApp.createTable("vogz", cols0);
		String [][] records_vogz = new String[82][cols0.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vogz[i][j] = genRandString();
			}
			DBApp.insert("vogz", records_vogz[i]);
			String tableTrace0 = DBApp.getLastTrace("vogz");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_vogz[i])+")", tableTrace0.contains(Arrays.toString(records_vogz[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test156TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"r74","g0s","oqc","k51","ce02","e7n5"};
		DBApp.createTable("l5", cols0);
		String [][] records_l5 = new String[47][cols0.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_l5[i][j] = genRandString();
			}
			DBApp.insert("l5", records_l5[i]);
			String tableTrace0 = DBApp.getLastTrace("l5");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_l5[i])+")", tableTrace0.contains(Arrays.toString(records_l5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"os11","esfs","rop","j2","drx8"};
		DBApp.createTable("sc0a7", cols1);
		String [][] records_sc0a7 = new String[3][cols1.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_sc0a7[i][j] = genRandString();
			}
			DBApp.insert("sc0a7", records_sc0a7[i]);
			String tableTrace1 = DBApp.getLastTrace("sc0a7");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_sc0a7[i])+")", tableTrace1.contains(Arrays.toString(records_sc0a7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"u2","b6t","q67","stx","a1b51","d6f","oa","lp7","yf00"};
		DBApp.createTable("y8c", cols2);
		String [][] records_y8c = new String[20][cols2.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_y8c[i][j] = genRandString();
			}
			DBApp.insert("y8c", records_y8c[i]);
			String tableTrace2 = DBApp.getLastTrace("y8c");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_y8c[i])+")", tableTrace2.contains(Arrays.toString(records_y8c[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"kpu","vc1"};
		DBApp.createTable("y7", cols3);
		String [][] records_y7 = new String[61][cols3.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_y7[i][j] = genRandString();
			}
			DBApp.insert("y7", records_y7[i]);
			String tableTrace3 = DBApp.getLastTrace("y7");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_y7[i])+")", tableTrace3.contains(Arrays.toString(records_y7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"ck2","f2","nto24","s0m","c2","tp","uv","i2p","f99","h4"};
		DBApp.createTable("w90", cols4);
		String [][] records_w90 = new String[82][cols4.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_w90[i][j] = genRandString();
			}
			DBApp.insert("w90", records_w90[i]);
			String tableTrace4 = DBApp.getLastTrace("w90");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_w90[i])+")", tableTrace4.contains(Arrays.toString(records_w90[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test157TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"gsa"};
		DBApp.createTable("h7", cols0);
		String [][] records_h7 = new String[81][cols0.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h7[i][j] = genRandString();
			}
			DBApp.insert("h7", records_h7[i]);
			String tableTrace0 = DBApp.getLastTrace("h7");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_h7[i])+")", tableTrace0.contains(Arrays.toString(records_h7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"ln3","b8sg"};
		DBApp.createTable("s9", cols1);
		String [][] records_s9 = new String[19][cols1.length];
		for(int i=0;i<19;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_s9[i][j] = genRandString();
			}
			DBApp.insert("s9", records_s9[i]);
			String tableTrace1 = DBApp.getLastTrace("s9");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_s9[i])+")", tableTrace1.contains(Arrays.toString(records_s9[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"cw33x","k3c","h6ger"};
		DBApp.createTable("t4", cols2);
		String [][] records_t4 = new String[11][cols2.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_t4[i][j] = genRandString();
			}
			DBApp.insert("t4", records_t4[i]);
			String tableTrace2 = DBApp.getLastTrace("t4");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_t4[i])+")", tableTrace2.contains(Arrays.toString(records_t4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"eg","m1","hd","r3156"};
		DBApp.createTable("no5", cols3);
		String [][] records_no5 = new String[34][cols3.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_no5[i][j] = genRandString();
			}
			DBApp.insert("no5", records_no5[i]);
			String tableTrace3 = DBApp.getLastTrace("no5");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_no5[i])+")", tableTrace3.contains(Arrays.toString(records_no5[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test158TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"flg9","qq2","u7","qry","cpz","csf","na611","el","jq8m","mbm"};
		DBApp.createTable("ryo", cols0);
		String [][] records_ryo = new String[96][cols0.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ryo[i][j] = genRandString();
			}
			DBApp.insert("ryo", records_ryo[i]);
			String tableTrace0 = DBApp.getLastTrace("ryo");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ryo[i])+")", tableTrace0.contains(Arrays.toString(records_ryo[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"d06w","a1u8","a1e","z5","ma"};
		DBApp.createTable("m9", cols1);
		String [][] records_m9 = new String[24][cols1.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_m9[i][j] = genRandString();
			}
			DBApp.insert("m9", records_m9[i]);
			String tableTrace1 = DBApp.getLastTrace("m9");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_m9[i])+")", tableTrace1.contains(Arrays.toString(records_m9[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"u547n","ugv","w36"};
		DBApp.createTable("uv16", cols2);
		String [][] records_uv16 = new String[29][cols2.length];
		for(int i=0;i<29;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_uv16[i][j] = genRandString();
			}
			DBApp.insert("uv16", records_uv16[i]);
			String tableTrace2 = DBApp.getLastTrace("uv16");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_uv16[i])+")", tableTrace2.contains(Arrays.toString(records_uv16[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test159TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"c1","sr","ti","k6476","gs04","tcl","x86ei","ggd","qf1","iuc"};
		DBApp.createTable("el7f", cols0);
		String [][] records_el7f = new String[59][cols0.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_el7f[i][j] = genRandString();
			}
			DBApp.insert("el7f", records_el7f[i]);
			String tableTrace0 = DBApp.getLastTrace("el7f");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_el7f[i])+")", tableTrace0.contains(Arrays.toString(records_el7f[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"u03","xfk","yf9","rf58","zdw60"};
		DBApp.createTable("f2b", cols1);
		String [][] records_f2b = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_f2b[i][j] = genRandString();
			}
			DBApp.insert("f2b", records_f2b[i]);
			String tableTrace1 = DBApp.getLastTrace("f2b");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_f2b[i])+")", tableTrace1.contains(Arrays.toString(records_f2b[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test160TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"m7g42","a8w","fpyl","q5g","k89fx","vji"};
		DBApp.createTable("k2l", cols0);
		String [][] records_k2l = new String[52][cols0.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k2l[i][j] = genRandString();
			}
			DBApp.insert("k2l", records_k2l[i]);
			String tableTrace0 = DBApp.getLastTrace("k2l");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_k2l[i])+")", tableTrace0.contains(Arrays.toString(records_k2l[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test161TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"g6","tf7","t7u"};
		DBApp.createTable("h00", cols0);
		String [][] records_h00 = new String[43][cols0.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h00[i][j] = genRandString();
			}
			DBApp.insert("h00", records_h00[i]);
			String tableTrace0 = DBApp.getLastTrace("h00");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_h00[i])+")", tableTrace0.contains(Arrays.toString(records_h00[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"oc0","d0274"};
		DBApp.createTable("x7v5", cols1);
		String [][] records_x7v5 = new String[44][cols1.length];
		for(int i=0;i<44;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_x7v5[i][j] = genRandString();
			}
			DBApp.insert("x7v5", records_x7v5[i]);
			String tableTrace1 = DBApp.getLastTrace("x7v5");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_x7v5[i])+")", tableTrace1.contains(Arrays.toString(records_x7v5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"mm2","s7n","hb9","zep","jwqn","m3","z8y56","i1y"};
		DBApp.createTable("yr", cols2);
		String [][] records_yr = new String[25][cols2.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_yr[i][j] = genRandString();
			}
			DBApp.insert("yr", records_yr[i]);
			String tableTrace2 = DBApp.getLastTrace("yr");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_yr[i])+")", tableTrace2.contains(Arrays.toString(records_yr[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"suy0","c29"};
		DBApp.createTable("jli", cols3);
		String [][] records_jli = new String[83][cols3.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_jli[i][j] = genRandString();
			}
			DBApp.insert("jli", records_jli[i]);
			String tableTrace3 = DBApp.getLastTrace("jli");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_jli[i])+")", tableTrace3.contains(Arrays.toString(records_jli[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test162TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"u52wv"};
		DBApp.createTable("px2", cols0);
		String [][] records_px2 = new String[81][cols0.length];
		for(int i=0;i<81;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_px2[i][j] = genRandString();
			}
			DBApp.insert("px2", records_px2[i]);
			String tableTrace0 = DBApp.getLastTrace("px2");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_px2[i])+")", tableTrace0.contains(Arrays.toString(records_px2[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"o48","ed0","jyd","z6l","evf","rqj6q","zh","re7b5"};
		DBApp.createTable("l53", cols1);
		String [][] records_l53 = new String[56][cols1.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_l53[i][j] = genRandString();
			}
			DBApp.insert("l53", records_l53[i]);
			String tableTrace1 = DBApp.getLastTrace("l53");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_l53[i])+")", tableTrace1.contains(Arrays.toString(records_l53[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test163TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"g1515","j9","b6","m1c","r3","h41","n8wz"};
		DBApp.createTable("bcc", cols0);
		String [][] records_bcc = new String[82][cols0.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_bcc[i][j] = genRandString();
			}
			DBApp.insert("bcc", records_bcc[i]);
			String tableTrace0 = DBApp.getLastTrace("bcc");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_bcc[i])+")", tableTrace0.contains(Arrays.toString(records_bcc[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"s90"};
		DBApp.createTable("n02", cols1);
		String [][] records_n02 = new String[47][cols1.length];
		for(int i=0;i<47;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_n02[i][j] = genRandString();
			}
			DBApp.insert("n02", records_n02[i]);
			String tableTrace1 = DBApp.getLastTrace("n02");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_n02[i])+")", tableTrace1.contains(Arrays.toString(records_n02[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"y89w","u6m7"};
		DBApp.createTable("qah", cols2);
		String [][] records_qah = new String[83][cols2.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_qah[i][j] = genRandString();
			}
			DBApp.insert("qah", records_qah[i]);
			String tableTrace2 = DBApp.getLastTrace("qah");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_qah[i])+")", tableTrace2.contains(Arrays.toString(records_qah[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"m5e","pb9","wwx"};
		DBApp.createTable("u5", cols3);
		String [][] records_u5 = new String[46][cols3.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_u5[i][j] = genRandString();
			}
			DBApp.insert("u5", records_u5[i]);
			String tableTrace3 = DBApp.getLastTrace("u5");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_u5[i])+")", tableTrace3.contains(Arrays.toString(records_u5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"o2c","ii91","lkv7","ndn","n8","v6j","ir"};
		DBApp.createTable("o2", cols4);
		String [][] records_o2 = new String[52][cols4.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_o2[i][j] = genRandString();
			}
			DBApp.insert("o2", records_o2[i]);
			String tableTrace4 = DBApp.getLastTrace("o2");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_o2[i])+")", tableTrace4.contains(Arrays.toString(records_o2[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test164TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"wc","w602d"};
		DBApp.createTable("xu", cols0);
		String [][] records_xu = new String[16][cols0.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_xu[i][j] = genRandString();
			}
			DBApp.insert("xu", records_xu[i]);
			String tableTrace0 = DBApp.getLastTrace("xu");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_xu[i])+")", tableTrace0.contains(Arrays.toString(records_xu[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test165TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"d3","r8d"};
		DBApp.createTable("q64ny", cols0);
		String [][] records_q64ny = new String[42][cols0.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_q64ny[i][j] = genRandString();
			}
			DBApp.insert("q64ny", records_q64ny[i]);
			String tableTrace0 = DBApp.getLastTrace("q64ny");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_q64ny[i])+")", tableTrace0.contains(Arrays.toString(records_q64ny[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"fqm9","hwdb","u37","n0b","w5r","bxr","w5","yhiy4"};
		DBApp.createTable("gw2", cols1);
		String [][] records_gw2 = new String[70][cols1.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_gw2[i][j] = genRandString();
			}
			DBApp.insert("gw2", records_gw2[i]);
			String tableTrace1 = DBApp.getLastTrace("gw2");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_gw2[i])+")", tableTrace1.contains(Arrays.toString(records_gw2[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"tk0"};
		DBApp.createTable("n44", cols2);
		String [][] records_n44 = new String[17][cols2.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_n44[i][j] = genRandString();
			}
			DBApp.insert("n44", records_n44[i]);
			String tableTrace2 = DBApp.getLastTrace("n44");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_n44[i])+")", tableTrace2.contains(Arrays.toString(records_n44[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test166TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"o63"};
		DBApp.createTable("xsl", cols0);
		String [][] records_xsl = new String[31][cols0.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_xsl[i][j] = genRandString();
			}
			DBApp.insert("xsl", records_xsl[i]);
			String tableTrace0 = DBApp.getLastTrace("xsl");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_xsl[i])+")", tableTrace0.contains(Arrays.toString(records_xsl[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"l1t","bh76","lgdd","o9q","yf1","yi6yq"};
		DBApp.createTable("o0de1", cols1);
		String [][] records_o0de1 = new String[5][cols1.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_o0de1[i][j] = genRandString();
			}
			DBApp.insert("o0de1", records_o0de1[i]);
			String tableTrace1 = DBApp.getLastTrace("o0de1");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_o0de1[i])+")", tableTrace1.contains(Arrays.toString(records_o0de1[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"avj","psp"};
		DBApp.createTable("e8l", cols2);
		String [][] records_e8l = new String[40][cols2.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_e8l[i][j] = genRandString();
			}
			DBApp.insert("e8l", records_e8l[i]);
			String tableTrace2 = DBApp.getLastTrace("e8l");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_e8l[i])+")", tableTrace2.contains(Arrays.toString(records_e8l[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"v5n","fz","awtqe","tf8d"};
		DBApp.createTable("f04v", cols3);
		String [][] records_f04v = new String[64][cols3.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_f04v[i][j] = genRandString();
			}
			DBApp.insert("f04v", records_f04v[i]);
			String tableTrace3 = DBApp.getLastTrace("f04v");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_f04v[i])+")", tableTrace3.contains(Arrays.toString(records_f04v[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"r979","x6","s3i","a73"};
		DBApp.createTable("z38at", cols4);
		String [][] records_z38at = new String[73][cols4.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_z38at[i][j] = genRandString();
			}
			DBApp.insert("z38at", records_z38at[i]);
			String tableTrace4 = DBApp.getLastTrace("z38at");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_z38at[i])+")", tableTrace4.contains(Arrays.toString(records_z38at[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test167TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"m18","v3","q91","nn00u","rvv","s9n3","blw","stts2"};
		DBApp.createTable("zw3n6", cols0);
		String [][] records_zw3n6 = new String[39][cols0.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_zw3n6[i][j] = genRandString();
			}
			DBApp.insert("zw3n6", records_zw3n6[i]);
			String tableTrace0 = DBApp.getLastTrace("zw3n6");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_zw3n6[i])+")", tableTrace0.contains(Arrays.toString(records_zw3n6[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"x3h","qm5","u6","syr","wguqx"};
		DBApp.createTable("h3", cols1);
		String [][] records_h3 = new String[80][cols1.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_h3[i][j] = genRandString();
			}
			DBApp.insert("h3", records_h3[i]);
			String tableTrace1 = DBApp.getLastTrace("h3");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_h3[i])+")", tableTrace1.contains(Arrays.toString(records_h3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"saw8"};
		DBApp.createTable("hf4a", cols2);
		String [][] records_hf4a = new String[12][cols2.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_hf4a[i][j] = genRandString();
			}
			DBApp.insert("hf4a", records_hf4a[i]);
			String tableTrace2 = DBApp.getLastTrace("hf4a");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_hf4a[i])+")", tableTrace2.contains(Arrays.toString(records_hf4a[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"l8m","f5lw","fvch"};
		DBApp.createTable("x89", cols3);
		String [][] records_x89 = new String[46][cols3.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_x89[i][j] = genRandString();
			}
			DBApp.insert("x89", records_x89[i]);
			String tableTrace3 = DBApp.getLastTrace("x89");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_x89[i])+")", tableTrace3.contains(Arrays.toString(records_x89[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test168TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"l1b","uz7","u03","g6u","nmx","wl","dw9","l38","xx","z8fm"};
		DBApp.createTable("x18", cols0);
		String [][] records_x18 = new String[68][cols0.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x18[i][j] = genRandString();
			}
			DBApp.insert("x18", records_x18[i]);
			String tableTrace0 = DBApp.getLastTrace("x18");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_x18[i])+")", tableTrace0.contains(Arrays.toString(records_x18[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test169TableInsertionPageTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"am","m2e6","wi","x7m","r0j","vhp8m","mjk","mr","zxm","w1og"};
		DBApp.createTable("pp2", cols0);
		String [][] records_pp2 = new String[39][cols0.length];
		for(int i=0;i<39;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_pp2[i][j] = genRandString();
			}
			DBApp.insert("pp2", records_pp2[i]);
			String tableTrace0 = DBApp.getLastTrace("pp2");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_pp2[i])+")", tableTrace0.contains(Arrays.toString(records_pp2[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"frb"};
		DBApp.createTable("x949", cols1);
		String [][] records_x949 = new String[33][cols1.length];
		for(int i=0;i<33;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_x949[i][j] = genRandString();
			}
			DBApp.insert("x949", records_x949[i]);
			String tableTrace1 = DBApp.getLastTrace("x949");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_x949[i])+")", tableTrace1.contains(Arrays.toString(records_x949[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"l9","vvi86","bu"};
		DBApp.createTable("ub8sf", cols2);
		String [][] records_ub8sf = new String[85][cols2.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ub8sf[i][j] = genRandString();
			}
			DBApp.insert("ub8sf", records_ub8sf[i]);
			String tableTrace2 = DBApp.getLastTrace("ub8sf");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_ub8sf[i])+")", tableTrace2.contains(Arrays.toString(records_ub8sf[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"o1d97","t85","m8x","j57","n56","l3m"};
		DBApp.createTable("gek", cols3);
		String [][] records_gek = new String[12][cols3.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_gek[i][j] = genRandString();
			}
			DBApp.insert("gek", records_gek[i]);
			String tableTrace3 = DBApp.getLastTrace("gek");
			assertTrue("Inserted records should appear in the table trace.("+Arrays.toString(records_gek[i])+")", tableTrace3.contains(Arrays.toString(records_gek[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test0TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("s38l3", cols0);
		String [][] records_s38l3 = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_s38l3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s38l3", records_s38l3[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("s38l3");
		for(int i=0;i<24;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_s38l3[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test1TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("x3", cols0);
		String [][] records_x3 = new String[43][cols0.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x3", records_x3[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("x3");
		for(int i=0;i<43;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_x3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("g55", cols1);
		String [][] records_g55 = new String[25][cols1.length];
		for(int i=0;i<25;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_g55[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g55", records_g55[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("g55");
		for(int i=0;i<25;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_g55[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("bn", cols2);
		String [][] records_bn = new String[10][cols2.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_bn[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bn", records_bn[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("bn");
		for(int i=0;i<10;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_bn[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test2TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("w7i7", cols0);
		String [][] records_w7i7 = new String[15][cols0.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_w7i7[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w7i7", records_w7i7[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("w7i7");
		for(int i=0;i<15;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_w7i7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("rm5u", cols1);
		String [][] records_rm5u = new String[77][cols1.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_rm5u[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rm5u", records_rm5u[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("rm5u");
		for(int i=0;i<77;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_rm5u[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("y3", cols2);
		String [][] records_y3 = new String[63][cols2.length];
		for(int i=0;i<63;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_y3[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y3", records_y3[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("y3");
		for(int i=0;i<63;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_y3[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test3TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("r41", cols0);
		String [][] records_r41 = new String[21][cols0.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r41[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r41", records_r41[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("r41");
		for(int i=0;i<21;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_r41[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("mq3", cols1);
		String [][] records_mq3 = new String[98][cols1.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_mq3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mq3", records_mq3[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("mq3");
		for(int i=0;i<98;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_mq3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("o91n", cols2);
		String [][] records_o91n = new String[60][cols2.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_o91n[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o91n", records_o91n[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("o91n");
		for(int i=0;i<60;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_o91n[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test4TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("vj8", cols0);
		String [][] records_vj8 = new String[80][cols0.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vj8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vj8", records_vj8[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("vj8");
		for(int i=0;i<80;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_vj8[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("n2", cols1);
		String [][] records_n2 = new String[42][cols1.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_n2[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n2", records_n2[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("n2");
		for(int i=0;i<42;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_n2[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test5TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("o7i8i", cols0);
		String [][] records_o7i8i = new String[93][cols0.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o7i8i[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o7i8i", records_o7i8i[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("o7i8i");
		for(int i=0;i<93;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_o7i8i[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("gb34m", cols1);
		String [][] records_gb34m = new String[69][cols1.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_gb34m[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gb34m", records_gb34m[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("gb34m");
		for(int i=0;i<69;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_gb34m[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test6TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("udett", cols0);
		String [][] records_udett = new String[20][cols0.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_udett[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("udett", records_udett[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("udett");
		for(int i=0;i<20;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_udett[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("bw03", cols1);
		String [][] records_bw03 = new String[27][cols1.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_bw03[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bw03", records_bw03[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("bw03");
		for(int i=0;i<27;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_bw03[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("z21", cols2);
		String [][] records_z21 = new String[94][cols2.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_z21[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z21", records_z21[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("z21");
		for(int i=0;i<94;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_z21[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("v73", cols3);
		String [][] records_v73 = new String[30][cols3.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_v73[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v73", records_v73[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("v73");
		for(int i=0;i<30;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_v73[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ex9h2", cols4);
		String [][] records_ex9h2 = new String[33][cols4.length];
		for(int i=0;i<33;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_ex9h2[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ex9h2", records_ex9h2[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("ex9h2");
		for(int i=0;i<33;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_ex9h2[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test7TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("m85", cols0);
		String [][] records_m85 = new String[15][cols0.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_m85[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m85", records_m85[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("m85");
		for(int i=0;i<15;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_m85[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test8TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("c0", cols0);
		String [][] records_c0 = new String[72][cols0.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_c0[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c0", records_c0[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("c0");
		for(int i=0;i<72;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_c0[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test9TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("s4", cols0);
		String [][] records_s4 = new String[74][cols0.length];
		for(int i=0;i<74;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_s4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s4", records_s4[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("s4");
		for(int i=0;i<74;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_s4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("qa71", cols1);
		String [][] records_qa71 = new String[93][cols1.length];
		for(int i=0;i<93;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_qa71[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qa71", records_qa71[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("qa71");
		for(int i=0;i<93;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_qa71[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("g3b7", cols2);
		String [][] records_g3b7 = new String[87][cols2.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_g3b7[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g3b7", records_g3b7[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("g3b7");
		for(int i=0;i<87;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_g3b7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("kf4y4", cols3);
		String [][] records_kf4y4 = new String[28][cols3.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_kf4y4[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kf4y4", records_kf4y4[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("kf4y4");
		for(int i=0;i<28;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_kf4y4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("g12", cols4);
		String [][] records_g12 = new String[83][cols4.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_g12[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g12", records_g12[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("g12");
		for(int i=0;i<83;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_g12[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test10TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("b91", cols0);
		String [][] records_b91 = new String[18][cols0.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_b91[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b91", records_b91[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("b91");
		for(int i=0;i<18;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_b91[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("a61", cols1);
		String [][] records_a61 = new String[83][cols1.length];
		for(int i=0;i<83;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_a61[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a61", records_a61[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("a61");
		for(int i=0;i<83;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_a61[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("yy6", cols2);
		String [][] records_yy6 = new String[45][cols2.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_yy6[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yy6", records_yy6[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("yy6");
		for(int i=0;i<45;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_yy6[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("n4c", cols3);
		String [][] records_n4c = new String[57][cols3.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_n4c[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n4c", records_n4c[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("n4c");
		for(int i=0;i<57;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_n4c[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("vcv", cols4);
		String [][] records_vcv = new String[65][cols4.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_vcv[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vcv", records_vcv[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("vcv");
		for(int i=0;i<65;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_vcv[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test11TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("lk", cols0);
		String [][] records_lk = new String[20][cols0.length];
		for(int i=0;i<20;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_lk[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lk", records_lk[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("lk");
		for(int i=0;i<20;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_lk[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("d6891", cols1);
		String [][] records_d6891 = new String[56][cols1.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_d6891[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d6891", records_d6891[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("d6891");
		for(int i=0;i<56;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_d6891[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("yoa2e", cols2);
		String [][] records_yoa2e = new String[75][cols2.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_yoa2e[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yoa2e", records_yoa2e[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("yoa2e");
		for(int i=0;i<75;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_yoa2e[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test12TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("gdc8", cols0);
		String [][] records_gdc8 = new String[87][cols0.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_gdc8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("gdc8", records_gdc8[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("gdc8");
		for(int i=0;i<87;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_gdc8[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("i6k6q", cols1);
		String [][] records_i6k6q = new String[54][cols1.length];
		for(int i=0;i<54;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_i6k6q[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i6k6q", records_i6k6q[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("i6k6q");
		for(int i=0;i<54;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_i6k6q[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("d2bl", cols2);
		String [][] records_d2bl = new String[18][cols2.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_d2bl[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d2bl", records_d2bl[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("d2bl");
		for(int i=0;i<18;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_d2bl[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("ztd", cols3);
		String [][] records_ztd = new String[89][cols3.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_ztd[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ztd", records_ztd[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("ztd");
		for(int i=0;i<89;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_ztd[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test13TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("f732", cols0);
		String [][] records_f732 = new String[7][cols0.length];
		for(int i=0;i<7;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_f732[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f732", records_f732[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("f732");
		for(int i=0;i<7;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_f732[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("l62", cols1);
		String [][] records_l62 = new String[73][cols1.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_l62[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l62", records_l62[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("l62");
		for(int i=0;i<73;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_l62[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("l7l", cols2);
		String [][] records_l7l = new String[75][cols2.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_l7l[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l7l", records_l7l[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("l7l");
		for(int i=0;i<75;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_l7l[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test14TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("z0x", cols0);
		String [][] records_z0x = new String[27][cols0.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_z0x[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z0x", records_z0x[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("z0x");
		for(int i=0;i<27;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_z0x[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("ajww", cols1);
		String [][] records_ajww = new String[59][cols1.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ajww[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ajww", records_ajww[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("ajww");
		for(int i=0;i<59;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_ajww[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("p4lx9", cols2);
		String [][] records_p4lx9 = new String[49][cols2.length];
		for(int i=0;i<49;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_p4lx9[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p4lx9", records_p4lx9[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("p4lx9");
		for(int i=0;i<49;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_p4lx9[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test15TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("yz", cols0);
		String [][] records_yz = new String[40][cols0.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_yz[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yz", records_yz[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("yz");
		for(int i=0;i<40;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_yz[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("hk7", cols1);
		String [][] records_hk7 = new String[78][cols1.length];
		for(int i=0;i<78;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_hk7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("hk7", records_hk7[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("hk7");
		for(int i=0;i<78;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_hk7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("wz", cols2);
		String [][] records_wz = new String[60][cols2.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_wz[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wz", records_wz[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("wz");
		for(int i=0;i<60;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_wz[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("j0y1y", cols3);
		String [][] records_j0y1y = new String[18][cols3.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_j0y1y[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j0y1y", records_j0y1y[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("j0y1y");
		for(int i=0;i<18;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_j0y1y[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("vi", cols4);
		String [][] records_vi = new String[2][cols4.length];
		for(int i=0;i<2;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_vi[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vi", records_vi[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("vi");
		for(int i=0;i<2;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_vi[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test16TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("d58", cols0);
		String [][] records_d58 = new String[3][cols0.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_d58[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d58", records_d58[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("d58");
		for(int i=0;i<3;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_d58[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("b9", cols1);
		String [][] records_b9 = new String[43][cols1.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_b9[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b9", records_b9[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("b9");
		for(int i=0;i<43;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_b9[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("z5z", cols2);
		String [][] records_z5z = new String[56][cols2.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_z5z[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z5z", records_z5z[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("z5z");
		for(int i=0;i<56;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_z5z[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("sg", cols3);
		String [][] records_sg = new String[21][cols3.length];
		for(int i=0;i<21;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_sg[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sg", records_sg[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("sg");
		for(int i=0;i<21;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_sg[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test17TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("pi", cols0);
		String [][] records_pi = new String[53][cols0.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_pi[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("pi", records_pi[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("pi");
		for(int i=0;i<53;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_pi[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("m18", cols1);
		String [][] records_m18 = new String[13][cols1.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_m18[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m18", records_m18[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("m18");
		for(int i=0;i<13;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_m18[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("j0", cols2);
		String [][] records_j0 = new String[71][cols2.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_j0[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j0", records_j0[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("j0");
		for(int i=0;i<71;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_j0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("b4", cols3);
		String [][] records_b4 = new String[13][cols3.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_b4[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b4", records_b4[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("b4");
		for(int i=0;i<13;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_b4[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test18TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("rh", cols0);
		String [][] records_rh = new String[77][cols0.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_rh[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rh", records_rh[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("rh");
		for(int i=0;i<77;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_rh[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("u62", cols1);
		String [][] records_u62 = new String[14][cols1.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_u62[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u62", records_u62[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("u62");
		for(int i=0;i<14;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_u62[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test19TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("jru", cols0);
		String [][] records_jru = new String[59][cols0.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_jru[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("jru", records_jru[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("jru");
		for(int i=0;i<59;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_jru[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("j9", cols1);
		String [][] records_j9 = new String[90][cols1.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_j9[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j9", records_j9[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("j9");
		for(int i=0;i<90;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_j9[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test20TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("w2", cols0);
		String [][] records_w2 = new String[57][cols0.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_w2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w2", records_w2[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("w2");
		for(int i=0;i<57;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_w2[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("nj", cols1);
		String [][] records_nj = new String[76][cols1.length];
		for(int i=0;i<76;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_nj[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("nj", records_nj[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("nj");
		for(int i=0;i<76;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_nj[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("q74", cols2);
		String [][] records_q74 = new String[79][cols2.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_q74[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("q74", records_q74[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("q74");
		for(int i=0;i<79;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_q74[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test21TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("e5u3o", cols0);
		String [][] records_e5u3o = new String[50][cols0.length];
		for(int i=0;i<50;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_e5u3o[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("e5u3o", records_e5u3o[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("e5u3o");
		for(int i=0;i<50;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_e5u3o[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("s4", cols1);
		String [][] records_s4 = new String[68][cols1.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_s4[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s4", records_s4[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("s4");
		for(int i=0;i<68;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_s4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("h9", cols2);
		String [][] records_h9 = new String[5][cols2.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_h9[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h9", records_h9[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("h9");
		for(int i=0;i<5;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_h9[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e"};
		DBApp.createTable("lgzx", cols3);
		String [][] records_lgzx = new String[69][cols3.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_lgzx[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lgzx", records_lgzx[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("lgzx");
		for(int i=0;i<69;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_lgzx[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("m1e", cols4);
		String [][] records_m1e = new String[65][cols4.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_m1e[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m1e", records_m1e[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("m1e");
		for(int i=0;i<65;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_m1e[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test22TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("i9", cols0);
		String [][] records_i9 = new String[26][cols0.length];
		for(int i=0;i<26;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i9[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i9", records_i9[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("i9");
		for(int i=0;i<26;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_i9[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test23TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("t9duh", cols0);
		String [][] records_t9duh = new String[51][cols0.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t9duh[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t9duh", records_t9duh[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("t9duh");
		for(int i=0;i<51;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_t9duh[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("mi9b", cols1);
		String [][] records_mi9b = new String[98][cols1.length];
		for(int i=0;i<98;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_mi9b[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("mi9b", records_mi9b[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("mi9b");
		for(int i=0;i<98;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_mi9b[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("oyd", cols2);
		String [][] records_oyd = new String[24][cols2.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_oyd[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("oyd", records_oyd[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("oyd");
		for(int i=0;i<24;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_oyd[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("bn2", cols3);
		String [][] records_bn2 = new String[70][cols3.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_bn2[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bn2", records_bn2[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("bn2");
		for(int i=0;i<70;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_bn2[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("w15", cols4);
		String [][] records_w15 = new String[16][cols4.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_w15[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w15", records_w15[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("w15");
		for(int i=0;i<16;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_w15[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test24TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("kb", cols0);
		String [][] records_kb = new String[75][cols0.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_kb[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kb", records_kb[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("kb");
		for(int i=0;i<75;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_kb[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test25TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("u5524", cols0);
		String [][] records_u5524 = new String[64][cols0.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u5524[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u5524", records_u5524[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("u5524");
		for(int i=0;i<64;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_u5524[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test26TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("k13", cols0);
		String [][] records_k13 = new String[72][cols0.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k13[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k13", records_k13[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("k13");
		for(int i=0;i<72;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_k13[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("x80", cols1);
		String [][] records_x80 = new String[57][cols1.length];
		for(int i=0;i<57;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_x80[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x80", records_x80[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("x80");
		for(int i=0;i<57;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_x80[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test27TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("kuk", cols0);
		String [][] records_kuk = new String[27][cols0.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_kuk[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kuk", records_kuk[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("kuk");
		for(int i=0;i<27;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_kuk[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("m3g6", cols1);
		String [][] records_m3g6 = new String[92][cols1.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_m3g6[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m3g6", records_m3g6[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("m3g6");
		for(int i=0;i<92;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_m3g6[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("z51c", cols2);
		String [][] records_z51c = new String[72][cols2.length];
		for(int i=0;i<72;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_z51c[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z51c", records_z51c[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("z51c");
		for(int i=0;i<72;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_z51c[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("fwz", cols3);
		String [][] records_fwz = new String[31][cols3.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_fwz[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fwz", records_fwz[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("fwz");
		for(int i=0;i<31;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_fwz[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("d81mc", cols4);
		String [][] records_d81mc = new String[38][cols4.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_d81mc[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d81mc", records_d81mc[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("d81mc");
		for(int i=0;i<38;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_d81mc[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test28TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("h0oq", cols0);
		String [][] records_h0oq = new String[60][cols0.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_h0oq[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h0oq", records_h0oq[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("h0oq");
		for(int i=0;i<60;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_h0oq[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("wc", cols1);
		String [][] records_wc = new String[38][cols1.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_wc[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wc", records_wc[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("wc");
		for(int i=0;i<38;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_wc[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test29TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ck", cols0);
		String [][] records_ck = new String[70][cols0.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_ck[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ck", records_ck[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("ck");
		for(int i=0;i<70;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_ck[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test30TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("v44", cols0);
		String [][] records_v44 = new String[77][cols0.length];
		for(int i=0;i<77;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_v44[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v44", records_v44[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("v44");
		for(int i=0;i<77;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_v44[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test31TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("d0s", cols0);
		String [][] records_d0s = new String[38][cols0.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_d0s[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d0s", records_d0s[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("d0s");
		for(int i=0;i<38;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_d0s[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("padx3", cols1);
		String [][] records_padx3 = new String[6][cols1.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_padx3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("padx3", records_padx3[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("padx3");
		for(int i=0;i<6;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_padx3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("yu1", cols2);
		String [][] records_yu1 = new String[35][cols2.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_yu1[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yu1", records_yu1[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("yu1");
		for(int i=0;i<35;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_yu1[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test32TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("t46", cols0);
		String [][] records_t46 = new String[55][cols0.length];
		for(int i=0;i<55;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t46[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t46", records_t46[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("t46");
		for(int i=0;i<55;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_t46[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("zb", cols1);
		String [][] records_zb = new String[91][cols1.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_zb[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zb", records_zb[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("zb");
		for(int i=0;i<91;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_zb[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("l96", cols2);
		String [][] records_l96 = new String[36][cols2.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_l96[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l96", records_l96[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("l96");
		for(int i=0;i<36;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_l96[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("t888y", cols3);
		String [][] records_t888y = new String[8][cols3.length];
		for(int i=0;i<8;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_t888y[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t888y", records_t888y[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("t888y");
		for(int i=0;i<8;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_t888y[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("bmo3o", cols4);
		String [][] records_bmo3o = new String[89][cols4.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_bmo3o[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bmo3o", records_bmo3o[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("bmo3o");
		for(int i=0;i<89;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_bmo3o[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test33TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("i5", cols0);
		String [][] records_i5 = new String[43][cols0.length];
		for(int i=0;i<43;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_i5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i5", records_i5[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("i5");
		for(int i=0;i<43;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_i5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("lah", cols1);
		String [][] records_lah = new String[90][cols1.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_lah[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lah", records_lah[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("lah");
		for(int i=0;i<90;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_lah[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("t6", cols2);
		String [][] records_t6 = new String[28][cols2.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_t6[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t6", records_t6[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("t6");
		for(int i=0;i<28;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_t6[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test34TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("lj8", cols0);
		String [][] records_lj8 = new String[28][cols0.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_lj8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("lj8", records_lj8[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("lj8");
		for(int i=0;i<28;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_lj8[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test35TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("v3p", cols0);
		String [][] records_v3p = new String[30][cols0.length];
		for(int i=0;i<30;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_v3p[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("v3p", records_v3p[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("v3p");
		for(int i=0;i<30;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_v3p[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("yg", cols1);
		String [][] records_yg = new String[79][cols1.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_yg[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yg", records_yg[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("yg");
		for(int i=0;i<79;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_yg[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test36TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("d88", cols0);
		String [][] records_d88 = new String[92][cols0.length];
		for(int i=0;i<92;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_d88[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d88", records_d88[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("d88");
		for(int i=0;i<92;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_d88[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("tyt1y", cols1);
		String [][] records_tyt1y = new String[34][cols1.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_tyt1y[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tyt1y", records_tyt1y[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("tyt1y");
		for(int i=0;i<34;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_tyt1y[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("itlmn", cols2);
		String [][] records_itlmn = new String[16][cols2.length];
		for(int i=0;i<16;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_itlmn[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("itlmn", records_itlmn[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("itlmn");
		for(int i=0;i<16;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_itlmn[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test37TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("p0", cols0);
		String [][] records_p0 = new String[66][cols0.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p0[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p0", records_p0[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("p0");
		for(int i=0;i<66;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_p0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("be0", cols1);
		String [][] records_be0 = new String[3][cols1.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_be0[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("be0", records_be0[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("be0");
		for(int i=0;i<3;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_be0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("ghdzd", cols2);
		String [][] records_ghdzd = new String[46][cols2.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_ghdzd[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ghdzd", records_ghdzd[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("ghdzd");
		for(int i=0;i<46;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_ghdzd[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("o6", cols3);
		String [][] records_o6 = new String[62][cols3.length];
		for(int i=0;i<62;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_o6[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o6", records_o6[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("o6");
		for(int i=0;i<62;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_o6[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ioeoq", cols4);
		String [][] records_ioeoq = new String[56][cols4.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_ioeoq[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ioeoq", records_ioeoq[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("ioeoq");
		for(int i=0;i<56;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_ioeoq[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test38TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("fr1", cols0);
		String [][] records_fr1 = new String[28][cols0.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_fr1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fr1", records_fr1[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("fr1");
		for(int i=0;i<28;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_fr1[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("rgd", cols1);
		String [][] records_rgd = new String[87][cols1.length];
		for(int i=0;i<87;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_rgd[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("rgd", records_rgd[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("rgd");
		for(int i=0;i<87;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_rgd[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("k6n", cols2);
		String [][] records_k6n = new String[48][cols2.length];
		for(int i=0;i<48;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_k6n[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k6n", records_k6n[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("k6n");
		for(int i=0;i<48;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_k6n[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d"};
		DBApp.createTable("dnhy", cols3);
		String [][] records_dnhy = new String[85][cols3.length];
		for(int i=0;i<85;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_dnhy[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dnhy", records_dnhy[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("dnhy");
		for(int i=0;i<85;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_dnhy[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("ef9", cols4);
		String [][] records_ef9 = new String[91][cols4.length];
		for(int i=0;i<91;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_ef9[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ef9", records_ef9[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("ef9");
		for(int i=0;i<91;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_ef9[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test39TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c"};
		DBApp.createTable("z3y", cols0);
		String [][] records_z3y = new String[35][cols0.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_z3y[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z3y", records_z3y[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("z3y");
		for(int i=0;i<35;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_z3y[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("n9a0", cols1);
		String [][] records_n9a0 = new String[52][cols1.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_n9a0[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n9a0", records_n9a0[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("n9a0");
		for(int i=0;i<52;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_n9a0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("n98", cols2);
		String [][] records_n98 = new String[34][cols2.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_n98[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("n98", records_n98[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("n98");
		for(int i=0;i<34;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_n98[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("a45", cols3);
		String [][] records_a45 = new String[42][cols3.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_a45[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a45", records_a45[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("a45");
		for(int i=0;i<42;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_a45[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f"};
		DBApp.createTable("r68", cols4);
		String [][] records_r68 = new String[22][cols4.length];
		for(int i=0;i<22;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_r68[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r68", records_r68[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("r68");
		for(int i=0;i<22;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_r68[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test40TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("cm2", cols0);
		String [][] records_cm2 = new String[31][cols0.length];
		for(int i=0;i<31;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_cm2[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cm2", records_cm2[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("cm2");
		for(int i=0;i<31;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_cm2[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("c4tm9", cols1);
		String [][] records_c4tm9 = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_c4tm9[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c4tm9", records_c4tm9[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("c4tm9");
		for(int i=0;i<40;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_c4tm9[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test41TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("vs4", cols0);
		String [][] records_vs4 = new String[51][cols0.length];
		for(int i=0;i<51;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vs4[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vs4", records_vs4[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("vs4");
		for(int i=0;i<51;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_vs4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("zfs6x", cols1);
		String [][] records_zfs6x = new String[70][cols1.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_zfs6x[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zfs6x", records_zfs6x[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("zfs6x");
		for(int i=0;i<70;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_zfs6x[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test42TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("al7", cols0);
		String [][] records_al7 = new String[67][cols0.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_al7[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("al7", records_al7[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("al7");
		for(int i=0;i<67;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_al7[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test43TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("u2n", cols0);
		String [][] records_u2n = new String[60][cols0.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_u2n[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u2n", records_u2n[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("u2n");
		for(int i=0;i<60;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_u2n[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("zm9dq", cols1);
		String [][] records_zm9dq = new String[3][cols1.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_zm9dq[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zm9dq", records_zm9dq[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("zm9dq");
		for(int i=0;i<3;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_zm9dq[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d"};
		DBApp.createTable("w4d3", cols2);
		String [][] records_w4d3 = new String[17][cols2.length];
		for(int i=0;i<17;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_w4d3[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w4d3", records_w4d3[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("w4d3");
		for(int i=0;i<17;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_w4d3[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test44TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("k8", cols0);
		String [][] records_k8 = new String[36][cols0.length];
		for(int i=0;i<36;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_k8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("k8", records_k8[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("k8");
		for(int i=0;i<36;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_k8[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("qc", cols1);
		String [][] records_qc = new String[38][cols1.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_qc[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qc", records_qc[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("qc");
		for(int i=0;i<38;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_qc[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test45TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("t9n8", cols0);
		String [][] records_t9n8 = new String[64][cols0.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t9n8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t9n8", records_t9n8[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("t9n8");
		for(int i=0;i<64;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_t9n8[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test46TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("db5", cols0);
		String [][] records_db5 = new String[38][cols0.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_db5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("db5", records_db5[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("db5");
		for(int i=0;i<38;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_db5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("bv3", cols1);
		String [][] records_bv3 = new String[56][cols1.length];
		for(int i=0;i<56;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_bv3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bv3", records_bv3[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("bv3");
		for(int i=0;i<56;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_bv3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("arg", cols2);
		String [][] records_arg = new String[70][cols2.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_arg[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("arg", records_arg[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("arg");
		for(int i=0;i<70;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_arg[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("w89u", cols3);
		String [][] records_w89u = new String[67][cols3.length];
		for(int i=0;i<67;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_w89u[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w89u", records_w89u[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("w89u");
		for(int i=0;i<67;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_w89u[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("wl12d", cols4);
		String [][] records_wl12d = new String[59][cols4.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_wl12d[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wl12d", records_wl12d[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("wl12d");
		for(int i=0;i<59;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_wl12d[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test47TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("p551n", cols0);
		String [][] records_p551n = new String[88][cols0.length];
		for(int i=0;i<88;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p551n[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p551n", records_p551n[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("p551n");
		for(int i=0;i<88;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_p551n[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("zv6", cols1);
		String [][] records_zv6 = new String[68][cols1.length];
		for(int i=0;i<68;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_zv6[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zv6", records_zv6[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("zv6");
		for(int i=0;i<68;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_zv6[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("bixh1", cols2);
		String [][] records_bixh1 = new String[75][cols2.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_bixh1[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bixh1", records_bixh1[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("bixh1");
		for(int i=0;i<75;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_bixh1[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("i2z", cols3);
		String [][] records_i2z = new String[71][cols3.length];
		for(int i=0;i<71;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_i2z[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i2z", records_i2z[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("i2z");
		for(int i=0;i<71;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_i2z[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("s65l5", cols4);
		String [][] records_s65l5 = new String[3][cols4.length];
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_s65l5[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s65l5", records_s65l5[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("s65l5");
		for(int i=0;i<3;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_s65l5[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test48TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("cy9", cols0);
		String [][] records_cy9 = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_cy9[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cy9", records_cy9[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("cy9");
		for(int i=0;i<24;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_cy9[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test49TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("f1", cols0);
		String [][] records_f1 = new String[22][cols0.length];
		for(int i=0;i<22;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_f1[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f1", records_f1[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("f1");
		for(int i=0;i<22;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_f1[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("bdb", cols1);
		String [][] records_bdb = new String[10][cols1.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_bdb[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bdb", records_bdb[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("bdb");
		for(int i=0;i<10;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_bdb[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test50TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("vqo", cols0);
		String [][] records_vqo = new String[11][cols0.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_vqo[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vqo", records_vqo[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("vqo");
		for(int i=0;i<11;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_vqo[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("ju4n0", cols1);
		String [][] records_ju4n0 = new String[24][cols1.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_ju4n0[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("ju4n0", records_ju4n0[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("ju4n0");
		for(int i=0;i<24;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_ju4n0[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("un4", cols2);
		String [][] records_un4 = new String[23][cols2.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_un4[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("un4", records_un4[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("un4");
		for(int i=0;i<23;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_un4[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("vfg", cols3);
		String [][] records_vfg = new String[37][cols3.length];
		for(int i=0;i<37;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_vfg[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("vfg", records_vfg[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("vfg");
		for(int i=0;i<37;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_vfg[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test51TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("o0of", cols0);
		String [][] records_o0of = new String[23][cols0.length];
		for(int i=0;i<23;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o0of[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o0of", records_o0of[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("o0of");
		for(int i=0;i<23;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_o0of[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("y49", cols1);
		String [][] records_y49 = new String[100][cols1.length];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_y49[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y49", records_y49[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("y49");
		for(int i=0;i<100;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_y49[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test52TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("dnp9", cols0);
		String [][] records_dnp9 = new String[75][cols0.length];
		for(int i=0;i<75;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_dnp9[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dnp9", records_dnp9[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("dnp9");
		for(int i=0;i<75;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_dnp9[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("f3", cols1);
		String [][] records_f3 = new String[73][cols1.length];
		for(int i=0;i<73;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_f3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("f3", records_f3[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("f3");
		for(int i=0;i<73;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_f3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("h4pn", cols2);
		String [][] records_h4pn = new String[97][cols2.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_h4pn[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("h4pn", records_h4pn[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("h4pn");
		for(int i=0;i<97;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_h4pn[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("l61", cols3);
		String [][] records_l61 = new String[69][cols3.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_l61[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l61", records_l61[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("l61");
		for(int i=0;i<69;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_l61[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test53TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("o26", cols0);
		String [][] records_o26 = new String[12][cols0.length];
		for(int i=0;i<12;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_o26[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o26", records_o26[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("o26");
		for(int i=0;i<12;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_o26[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("j70c8", cols1);
		String [][] records_j70c8 = new String[45][cols1.length];
		for(int i=0;i<45;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_j70c8[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j70c8", records_j70c8[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("j70c8");
		for(int i=0;i<45;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_j70c8[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test54TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("tq6", cols0);
		String [][] records_tq6 = new String[42][cols0.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_tq6[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("tq6", records_tq6[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("tq6");
		for(int i=0;i<42;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_tq6[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("fg", cols1);
		String [][] records_fg = new String[76][cols1.length];
		for(int i=0;i<76;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_fg[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fg", records_fg[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("fg");
		for(int i=0;i<76;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_fg[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("u07", cols2);
		String [][] records_u07 = new String[6][cols2.length];
		for(int i=0;i<6;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_u07[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u07", records_u07[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("u07");
		for(int i=0;i<6;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_u07[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("kbs6", cols3);
		String [][] records_kbs6 = new String[28][cols3.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_kbs6[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("kbs6", records_kbs6[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("kbs6");
		for(int i=0;i<28;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_kbs6[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test55TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("p3", cols0);
		String [][] records_p3 = new String[52][cols0.length];
		for(int i=0;i<52;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_p3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("p3", records_p3[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("p3");
		for(int i=0;i<52;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_p3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a"};
		DBApp.createTable("d2a", cols1);
		String [][] records_d2a = new String[79][cols1.length];
		for(int i=0;i<79;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_d2a[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d2a", records_d2a[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("d2a");
		for(int i=0;i<79;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_d2a[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test56TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("b9", cols0);
		String [][] records_b9 = new String[11][cols0.length];
		for(int i=0;i<11;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_b9[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("b9", records_b9[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("b9");
		for(int i=0;i<11;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_b9[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("qh1", cols1);
		String [][] records_qh1 = new String[96][cols1.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_qh1[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qh1", records_qh1[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("qh1");
		for(int i=0;i<96;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_qh1[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("av4f", cols2);
		String [][] records_av4f = new String[89][cols2.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_av4f[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("av4f", records_av4f[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("av4f");
		for(int i=0;i<89;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_av4f[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test57TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("m93ga", cols0);
		String [][] records_m93ga = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_m93ga[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m93ga", records_m93ga[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("m93ga");
		for(int i=0;i<24;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_m93ga[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m2", cols1);
		String [][] records_m2 = new String[90][cols1.length];
		for(int i=0;i<90;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_m2[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m2", records_m2[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("m2");
		for(int i=0;i<90;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_m2[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test58TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("fn391", cols0);
		String [][] records_fn391 = new String[96][cols0.length];
		for(int i=0;i<96;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_fn391[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fn391", records_fn391[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("fn391");
		for(int i=0;i<96;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_fn391[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("z5n19", cols1);
		String [][] records_z5n19 = new String[89][cols1.length];
		for(int i=0;i<89;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_z5n19[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z5n19", records_z5n19[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("z5n19");
		for(int i=0;i<89;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_z5n19[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a"};
		DBApp.createTable("dy15e", cols2);
		String [][] records_dy15e = new String[46][cols2.length];
		for(int i=0;i<46;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_dy15e[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dy15e", records_dy15e[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("dy15e");
		for(int i=0;i<46;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_dy15e[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test59TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("t0w8", cols0);
		String [][] records_t0w8 = new String[27][cols0.length];
		for(int i=0;i<27;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_t0w8[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("t0w8", records_t0w8[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("t0w8");
		for(int i=0;i<27;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_t0w8[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test60TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("bi39", cols0);
		String [][] records_bi39 = new String[80][cols0.length];
		for(int i=0;i<80;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_bi39[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bi39", records_bi39[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("bi39");
		for(int i=0;i<80;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_bi39[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b"};
		DBApp.createTable("cu", cols1);
		String [][] records_cu = new String[82][cols1.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_cu[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("cu", records_cu[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("cu");
		for(int i=0;i<82;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_cu[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("s3", cols2);
		String [][] records_s3 = new String[8][cols2.length];
		for(int i=0;i<8;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_s3[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("s3", records_s3[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("s3");
		for(int i=0;i<8;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_s3[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test61TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a"};
		DBApp.createTable("qgl3", cols0);
		String [][] records_qgl3 = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_qgl3[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("qgl3", records_qgl3[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("qgl3");
		for(int i=0;i<32;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_qgl3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("i9z3", cols1);
		String [][] records_i9z3 = new String[55][cols1.length];
		for(int i=0;i<55;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_i9z3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("i9z3", records_i9z3[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("i9z3");
		for(int i=0;i<55;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_i9z3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("x19", cols2);
		String [][] records_x19 = new String[61][cols2.length];
		for(int i=0;i<61;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_x19[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x19", records_x19[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("x19");
		for(int i=0;i<61;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_x19[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a"};
		DBApp.createTable("g7", cols3);
		String [][] records_g7 = new String[10][cols3.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_g7[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g7", records_g7[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("g7");
		for(int i=0;i<10;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_g7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("o0d1", cols4);
		String [][] records_o0d1 = new String[34][cols4.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols4.length;j++)
			{
				records_o0d1[i][j] = cols4[j]+(i+(i%(j+1)));
			}
			DBApp.insert("o0d1", records_o0d1[i]);
		}
		ArrayList<String []> tableSelect4 = DBApp.select("o0d1");
		for(int i=0;i<34;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect4.get(i)).equals(Arrays.toString(records_o0d1[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test62TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f"};
		DBApp.createTable("sjq", cols0);
		String [][] records_sjq = new String[53][cols0.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sjq[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sjq", records_sjq[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("sjq");
		for(int i=0;i<53;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_sjq[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("w2s", cols1);
		String [][] records_w2s = new String[13][cols1.length];
		for(int i=0;i<13;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_w2s[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w2s", records_w2s[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("w2s");
		for(int i=0;i<13;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_w2s[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("yc6h", cols2);
		String [][] records_yc6h = new String[42][cols2.length];
		for(int i=0;i<42;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_yc6h[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yc6h", records_yc6h[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("yc6h");
		for(int i=0;i<42;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_yc6h[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test63TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("x5p", cols0);
		String [][] records_x5p = new String[4][cols0.length];
		for(int i=0;i<4;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x5p[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x5p", records_x5p[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("x5p");
		for(int i=0;i<4;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_x5p[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test64TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("r6sji", cols0);
		String [][] records_r6sji = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_r6sji[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r6sji", records_r6sji[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("r6sji");
		for(int i=0;i<10;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_r6sji[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("l7", cols1);
		String [][] records_l7 = new String[70][cols1.length];
		for(int i=0;i<70;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_l7[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("l7", records_l7[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("l7");
		for(int i=0;i<70;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_l7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b"};
		DBApp.createTable("iek", cols2);
		String [][] records_iek = new String[38][cols2.length];
		for(int i=0;i<38;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_iek[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("iek", records_iek[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("iek");
		for(int i=0;i<38;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_iek[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test65TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("do", cols0);
		String [][] records_do = new String[64][cols0.length];
		for(int i=0;i<64;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_do[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("do", records_do[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("do");
		for(int i=0;i<64;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_do[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("a5", cols1);
		String [][] records_a5 = new String[82][cols1.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_a5[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a5", records_a5[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("a5");
		for(int i=0;i<82;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_a5[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e"};
		DBApp.createTable("y1", cols2);
		String [][] records_y1 = new String[59][cols2.length];
		for(int i=0;i<59;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_y1[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("y1", records_y1[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("y1");
		for(int i=0;i<59;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_y1[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test66TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("a1hr", cols0);
		String [][] records_a1hr = new String[34][cols0.length];
		for(int i=0;i<34;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_a1hr[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("a1hr", records_a1hr[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("a1hr");
		for(int i=0;i<34;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_a1hr[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d"};
		DBApp.createTable("auzt6", cols1);
		String [][] records_auzt6 = new String[97][cols1.length];
		for(int i=0;i<97;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_auzt6[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("auzt6", records_auzt6[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("auzt6");
		for(int i=0;i<97;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_auzt6[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c"};
		DBApp.createTable("u82", cols2);
		String [][] records_u82 = new String[40][cols2.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_u82[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("u82", records_u82[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("u82");
		for(int i=0;i<40;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_u82[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f"};
		DBApp.createTable("z47f", cols3);
		String [][] records_z47f = new String[66][cols3.length];
		for(int i=0;i<66;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_z47f[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z47f", records_z47f[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("z47f");
		for(int i=0;i<66;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_z47f[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test67TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e"};
		DBApp.createTable("fl6", cols0);
		String [][] records_fl6 = new String[94][cols0.length];
		for(int i=0;i<94;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_fl6[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("fl6", records_fl6[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("fl6");
		for(int i=0;i<94;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_fl6[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("z75t", cols1);
		String [][] records_z75t = new String[15][cols1.length];
		for(int i=0;i<15;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_z75t[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("z75t", records_z75t[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("z75t");
		for(int i=0;i<15;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_z75t[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test68TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b"};
		DBApp.createTable("zvc", cols0);
		String [][] records_zvc = new String[82][cols0.length];
		for(int i=0;i<82;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_zvc[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zvc", records_zvc[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("zvc");
		for(int i=0;i<82;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_zvc[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f"};
		DBApp.createTable("d0b", cols1);
		String [][] records_d0b = new String[99][cols1.length];
		for(int i=0;i<99;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_d0b[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("d0b", records_d0b[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("d0b");
		for(int i=0;i<99;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_d0b[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test69TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d"};
		DBApp.createTable("zlnx", cols0);
		String [][] records_zlnx = new String[35][cols0.length];
		for(int i=0;i<35;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_zlnx[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("zlnx", records_zlnx[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("zlnx");
		for(int i=0;i<35;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_zlnx[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("wv6", cols1);
		String [][] records_wv6 = new String[86][cols1.length];
		for(int i=0;i<86;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_wv6[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("wv6", records_wv6[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("wv6");
		for(int i=0;i<86;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_wv6[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test70TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("x5", cols0);
		String [][] records_x5 = new String[69][cols0.length];
		for(int i=0;i<69;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_x5[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("x5", records_x5[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("x5");
		for(int i=0;i<69;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_x5[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test71TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("j6m", cols0);
		String [][] records_j6m = new String[9][cols0.length];
		for(int i=0;i<9;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_j6m[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("j6m", records_j6m[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("j6m");
		for(int i=0;i<9;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_j6m[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c"};
		DBApp.createTable("bw3", cols1);
		String [][] records_bw3 = new String[65][cols1.length];
		for(int i=0;i<65;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_bw3[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("bw3", records_bw3[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("bw3");
		for(int i=0;i<65;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_bw3[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f"};
		DBApp.createTable("w21", cols2);
		String [][] records_w21 = new String[40][cols2.length];
		for(int i=0;i<40;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_w21[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("w21", records_w21[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("w21");
		for(int i=0;i<40;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_w21[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test72TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("sw", cols0);
		String [][] records_sw = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_sw[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("sw", records_sw[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("sw");
		for(int i=0;i<10;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_sw[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("r5u", cols1);
		String [][] records_r5u = new String[14][cols1.length];
		for(int i=0;i<14;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_r5u[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("r5u", records_r5u[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("r5u");
		for(int i=0;i<14;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_r5u[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test73TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("dbw6s", cols0);
		String [][] records_dbw6s = new String[53][cols0.length];
		for(int i=0;i<53;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_dbw6s[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("dbw6s", records_dbw6s[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("dbw6s");
		for(int i=0;i<53;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_dbw6s[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("c2m", cols1);
		String [][] records_c2m = new String[5][cols1.length];
		for(int i=0;i<5;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_c2m[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("c2m", records_c2m[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("c2m");
		for(int i=0;i<5;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_c2m[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test74TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("of7", cols0);
		String [][] records_of7 = new String[28][cols0.length];
		for(int i=0;i<28;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_of7[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("of7", records_of7[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("of7");
		for(int i=0;i<28;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_of7[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e"};
		DBApp.createTable("yb", cols1);
		String [][] records_yb = new String[10][cols1.length];
		for(int i=0;i<10;i++)
		{
			for(int j=0;j<cols1.length;j++)
			{
				records_yb[i][j] = cols1[j]+(i+(i%(j+1)));
			}
			DBApp.insert("yb", records_yb[i]);
		}
		ArrayList<String []> tableSelect1 = DBApp.select("yb");
		for(int i=0;i<10;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect1.get(i)).equals(Arrays.toString(records_yb[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("uc3y9", cols2);
		String [][] records_uc3y9 = new String[48][cols2.length];
		for(int i=0;i<48;i++)
		{
			for(int j=0;j<cols2.length;j++)
			{
				records_uc3y9[i][j] = cols2[j]+(i+(i%(j+1)));
			}
			DBApp.insert("uc3y9", records_uc3y9[i]);
		}
		ArrayList<String []> tableSelect2 = DBApp.select("uc3y9");
		for(int i=0;i<48;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect2.get(i)).equals(Arrays.toString(records_uc3y9[i])));
		}
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c"};
		DBApp.createTable("m615", cols3);
		String [][] records_m615 = new String[18][cols3.length];
		for(int i=0;i<18;i++)
		{
			for(int j=0;j<cols3.length;j++)
			{
				records_m615[i][j] = cols3[j]+(i+(i%(j+1)));
			}
			DBApp.insert("m615", records_m615[i]);
		}
		ArrayList<String []> tableSelect3 = DBApp.select("m615");
		for(int i=0;i<18;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect3.get(i)).equals(Arrays.toString(records_m615[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test75TableInsertionSelectionDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("g47z", cols0);
		String [][] records_g47z = new String[60][cols0.length];
		for(int i=0;i<60;i++)
		{
			for(int j=0;j<cols0.length;j++)
			{
				records_g47z[i][j] = cols0[j]+(i+(i%(j+1)));
			}
			DBApp.insert("g47z", records_g47z[i]);
		}
		ArrayList<String []> tableSelect0 = DBApp.select("g47z");
		for(int i=0;i<60;i++)
		{
			assertTrue("Inserted and selected records should be equivalent.", Arrays.toString(tableSelect0.get(i)).equals(Arrays.toString(records_g47z[i])));
		}
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(997);
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