package com.dbapp;
import static org.junit.Assert.assertTrue;
import java.util.*;
import org.junit.Test;
public class DBAppTests02
{

	@Test(timeout = 10000)
	public void test139TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"p4u","ez0","oiv","v514","a2p","kt211","b30","c3m40","w69"};
		DBApp.createTable("uh7", cols0);
		String tableTrace0 = DBApp.getLastTrace("uh7");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("uh7"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"e83","m1","hj8","hn71e","qp98k","g28pe","q4z","hk5"};
		DBApp.createTable("ewn", cols1);
		String tableTrace1 = DBApp.getLastTrace("ewn");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("ewn"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"gx8","n576","m3","k1e","y6d","lw"};
		DBApp.createTable("x89d", cols2);
		String tableTrace2 = DBApp.getLastTrace("x89d");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("x89d"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"n9","sdr2","b40","s96e8","skg","f5s","tz0w7","n60"};
		DBApp.createTable("j66", cols3);
		String tableTrace3 = DBApp.getLastTrace("j66");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("j66"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"e31n","u82v5","i70","x5x2u","b0","tqm","r72","y3g","sf4d","xk5"};
		DBApp.createTable("pf9g", cols4);
		String tableTrace4 = DBApp.getLastTrace("pf9g");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("pf9g"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test140TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"pnyn","w15x","fem","m544"};
		DBApp.createTable("s5m", cols0);
		String tableTrace0 = DBApp.getLastTrace("s5m");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("s5m"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"c84","w3t0","bk"};
		DBApp.createTable("wr6xm", cols1);
		String tableTrace1 = DBApp.getLastTrace("wr6xm");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("wr6xm"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test141TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"yd6","mjr","wna3","om"};
		DBApp.createTable("q54m", cols0);
		String tableTrace0 = DBApp.getLastTrace("q54m");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("q54m"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"h6","o7","r062","gmc","dl338","sok","m146","lb","u5n","n2g0t"};
		DBApp.createTable("uo", cols1);
		String tableTrace1 = DBApp.getLastTrace("uo");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("uo"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"ux","f8d","d8g","mqk1","u4w","f3i3"};
		DBApp.createTable("m8cd9", cols2);
		String tableTrace2 = DBApp.getLastTrace("m8cd9");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("m8cd9"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test142TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"qpn","z35","vi6","gxq","j2r4","x8","ws","p27b"};
		DBApp.createTable("ai5", cols0);
		String tableTrace0 = DBApp.getLastTrace("ai5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("ai5"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"e81r","rjb","v8","v1v4","pir"};
		DBApp.createTable("dh8", cols1);
		String tableTrace1 = DBApp.getLastTrace("dh8");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("dh8"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"f1558","axaa","wc65"};
		DBApp.createTable("i22", cols2);
		String tableTrace2 = DBApp.getLastTrace("i22");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("i22"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"hb","tm7y","p0","l79","kk48","w4fdm","h9p4f","a424"};
		DBApp.createTable("m69f", cols3);
		String tableTrace3 = DBApp.getLastTrace("m69f");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("m69f"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"jte90","te"};
		DBApp.createTable("rwt", cols4);
		String tableTrace4 = DBApp.getLastTrace("rwt");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("rwt"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test143TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"q73qx","apr"};
		DBApp.createTable("cy9", cols0);
		String tableTrace0 = DBApp.getLastTrace("cy9");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("cy9"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"qd","t5trt","mx6","iavwd","u9z","ycza","d7"};
		DBApp.createTable("vz91x", cols1);
		String tableTrace1 = DBApp.getLastTrace("vz91x");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("vz91x"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test144TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"xmz","v69i4"};
		DBApp.createTable("vp3", cols0);
		String tableTrace0 = DBApp.getLastTrace("vp3");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("vp3"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"v00e4","ti44","fi6s","pe3"};
		DBApp.createTable("c4s97", cols1);
		String tableTrace1 = DBApp.getLastTrace("c4s97");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("c4s97"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test145TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"z52","s458b","xsi"};
		DBApp.createTable("my89", cols0);
		String tableTrace0 = DBApp.getLastTrace("my89");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("my89"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"k1x","bbo","fiy9n","xpi5","b64n5","y9d1"};
		DBApp.createTable("o98w", cols1);
		String tableTrace1 = DBApp.getLastTrace("o98w");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("o98w"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test146TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"h998","d98","t4","bjj38","k1y","k87","jb9","e385"};
		DBApp.createTable("s26o", cols0);
		String tableTrace0 = DBApp.getLastTrace("s26o");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("s26o"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test147TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"tr","v56h","wu75","kx09"};
		DBApp.createTable("z89", cols0);
		String tableTrace0 = DBApp.getLastTrace("z89");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("z89"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"er5","o2o60","jr1"};
		DBApp.createTable("lw87", cols1);
		String tableTrace1 = DBApp.getLastTrace("lw87");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("lw87"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"nkj","hx1j","utfl7","f83n"};
		DBApp.createTable("whif2", cols2);
		String tableTrace2 = DBApp.getLastTrace("whif2");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("whif2"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"wx5y7","z65zy","v4p08","t186","p0oq1","gbv","k336","gl44","l0z","i9p5"};
		DBApp.createTable("l59", cols3);
		String tableTrace3 = DBApp.getLastTrace("l59");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("l59"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test148TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a4ha","jq50","w5","k6u","o82v","dp5","xq6o7","u18","rlar7","w6"};
		DBApp.createTable("a1y", cols0);
		String tableTrace0 = DBApp.getLastTrace("a1y");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("a1y"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"uj2sa","b56z","hch0","x4","j1","bu3j","jac4","nsag2","r0","zx8"};
		DBApp.createTable("y2sf2", cols1);
		String tableTrace1 = DBApp.getLastTrace("y2sf2");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("y2sf2"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test149TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"t41m","vhd"};
		DBApp.createTable("i9", cols0);
		String tableTrace0 = DBApp.getLastTrace("i9");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("i9"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"zo61","ht","k04","u30","q725t"};
		DBApp.createTable("r6t1", cols1);
		String tableTrace1 = DBApp.getLastTrace("r6t1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("r6t1"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"nle3","b632x","yd2","ll","dez"};
		DBApp.createTable("bky", cols2);
		String tableTrace2 = DBApp.getLastTrace("bky");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("bky"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"a067y","x4z7","w067a","i77","f205q","j8"};
		DBApp.createTable("d3f", cols3);
		String tableTrace3 = DBApp.getLastTrace("d3f");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("d3f"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test150TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"dk","k087"};
		DBApp.createTable("qml", cols0);
		String tableTrace0 = DBApp.getLastTrace("qml");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("qml"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"o157","ahk","wao","l92"};
		DBApp.createTable("xq6", cols1);
		String tableTrace1 = DBApp.getLastTrace("xq6");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("xq6"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test151TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"ne","c95j","q1h1","mj","d3c","y0qj","zqu"};
		DBApp.createTable("g43", cols0);
		String tableTrace0 = DBApp.getLastTrace("g43");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("g43"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test152TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"redw","h8","s0","n92","z8jj0","bg8h","um51","co23"};
		DBApp.createTable("d1", cols0);
		String tableTrace0 = DBApp.getLastTrace("d1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("d1"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"s1pot","tk","zx5","kj73","i9n"};
		DBApp.createTable("wh1", cols1);
		String tableTrace1 = DBApp.getLastTrace("wh1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("wh1"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"b25","au","mt486","mz","i6","m0"};
		DBApp.createTable("wf", cols2);
		String tableTrace2 = DBApp.getLastTrace("wf");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("wf"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"ktw7h","a1","ly","xje","co171","o4t"};
		DBApp.createTable("zj2w0", cols3);
		String tableTrace3 = DBApp.getLastTrace("zj2w0");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("zj2w0"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"v0h","l030","c3c","piesr","ve","lo"};
		DBApp.createTable("sr32", cols4);
		String tableTrace4 = DBApp.getLastTrace("sr32");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("sr32"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test153TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"r1s","xm8","sv","j2k"};
		DBApp.createTable("j06", cols0);
		String tableTrace0 = DBApp.getLastTrace("j06");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("j06"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"af12a","n89cr","j50","p74f9"};
		DBApp.createTable("d84", cols1);
		String tableTrace1 = DBApp.getLastTrace("d84");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("d84"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"v1","s2sn","o1","c58c","c6b9","h8"};
		DBApp.createTable("y52", cols2);
		String tableTrace2 = DBApp.getLastTrace("y52");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("y52"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"c2p","gk8n","zqhb","zn6","a09","c49ww","d763","wd4"};
		DBApp.createTable("h6rpe", cols3);
		String tableTrace3 = DBApp.getLastTrace("h6rpe");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("h6rpe"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test154TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"kk87","w3","l0","w46","k2","ro","pvcd","pyyr","a567","v5w"};
		DBApp.createTable("ej66", cols0);
		String tableTrace0 = DBApp.getLastTrace("ej66");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("ej66"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"n18","yj71","vu8","k97or"};
		DBApp.createTable("wl7", cols1);
		String tableTrace1 = DBApp.getLastTrace("wl7");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("wl7"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"p48p","sxe"};
		DBApp.createTable("ghh2", cols2);
		String tableTrace2 = DBApp.getLastTrace("ghh2");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("ghh2"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"rt13","hl","b9j"};
		DBApp.createTable("cah2", cols3);
		String tableTrace3 = DBApp.getLastTrace("cah2");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("cah2"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test155TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"y3f9","t9vm","gv"};
		DBApp.createTable("y298", cols0);
		String tableTrace0 = DBApp.getLastTrace("y298");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("y298"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"k8t3","y31"};
		DBApp.createTable("ab93", cols1);
		String tableTrace1 = DBApp.getLastTrace("ab93");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("ab93"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"mfa","q7z","ox7i","e73","p5501","rnl","qv7"};
		DBApp.createTable("g3p9", cols2);
		String tableTrace2 = DBApp.getLastTrace("g3p9");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("g3p9"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"d1u6","dnt"};
		DBApp.createTable("m33ks", cols3);
		String tableTrace3 = DBApp.getLastTrace("m33ks");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("m33ks"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"zt","mo","bwh0f","ct1","u4","kj7h","l9t46","m8k","z337"};
		DBApp.createTable("hhe", cols4);
		String tableTrace4 = DBApp.getLastTrace("hhe");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("hhe"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test156TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"y5","n15","t8b6","kwt","fe1"};
		DBApp.createTable("yr0", cols0);
		String tableTrace0 = DBApp.getLastTrace("yr0");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("yr0"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"u7q3x","v4wi","ysl","u0weg","dtn","ptt","k08"};
		DBApp.createTable("crx8", cols1);
		String tableTrace1 = DBApp.getLastTrace("crx8");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("crx8"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"w35hq","ss","z0","q3aw"};
		DBApp.createTable("unv", cols2);
		String tableTrace2 = DBApp.getLastTrace("unv");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("unv"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"pfmd","cx0"};
		DBApp.createTable("kdfc", cols3);
		String tableTrace3 = DBApp.getLastTrace("kdfc");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("kdfc"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test157TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"rj1","muk","jyp","nf1u","tr","cq"};
		DBApp.createTable("w92", cols0);
		String tableTrace0 = DBApp.getLastTrace("w92");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("w92"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"h2y","yn0","jy9","cyep","z7b6","cb","mle"};
		DBApp.createTable("qx1", cols1);
		String tableTrace1 = DBApp.getLastTrace("qx1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("qx1"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"my9","l9"};
		DBApp.createTable("tk", cols2);
		String tableTrace2 = DBApp.getLastTrace("tk");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("tk"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test158TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"yp","oxom","t5g","lz31","kj642","ck","r17z"};
		DBApp.createTable("c9", cols0);
		String tableTrace0 = DBApp.getLastTrace("c9");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("c9"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"dvh","a468i","r04","occ"};
		DBApp.createTable("oq", cols1);
		String tableTrace1 = DBApp.getLastTrace("oq");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("oq"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"nz31","ibo","i2v","fb6"};
		DBApp.createTable("v527g", cols2);
		String tableTrace2 = DBApp.getLastTrace("v527g");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("v527g"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"sbc","k5p","jfxi","tj29r","m936","oe2","ir752","p7o1","z52","voz"};
		DBApp.createTable("ims", cols3);
		String tableTrace3 = DBApp.getLastTrace("ims");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("ims"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"v26","w30","hj7","ik","uyr","m72b4","rg0j6","a7x"};
		DBApp.createTable("s2", cols4);
		String tableTrace4 = DBApp.getLastTrace("s2");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("s2"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test159TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"n4","v8p6","rzi","lr1o","e8igr"};
		DBApp.createTable("mge", cols0);
		String tableTrace0 = DBApp.getLastTrace("mge");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("mge"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"ze6w","f18","io4","o44","b16","fts","cqi","p4z"};
		DBApp.createTable("i29f2", cols1);
		String tableTrace1 = DBApp.getLastTrace("i29f2");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("i29f2"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test160TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"n3z","ns18","z915","rqh","k88x","iaf","c5a","kw","e8o","l6w9"};
		DBApp.createTable("r4", cols0);
		String tableTrace0 = DBApp.getLastTrace("r4");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("r4"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"mw24w","fmy","zc6s9","p92b0","wdg","d0","mu44u","wo","yn2m8","z75"};
		DBApp.createTable("y1", cols1);
		String tableTrace1 = DBApp.getLastTrace("y1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("y1"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"h33"};
		DBApp.createTable("i6", cols2);
		String tableTrace2 = DBApp.getLastTrace("i6");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("i6"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test161TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"iv7","vp5","p4","s3y3","wf","f7x0b","tn3","pjs"};
		DBApp.createTable("p9bd", cols0);
		String tableTrace0 = DBApp.getLastTrace("p9bd");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("p9bd"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"n3zq","re","r5d","uv","hy","lgp"};
		DBApp.createTable("w3s", cols1);
		String tableTrace1 = DBApp.getLastTrace("w3s");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("w3s"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test162TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"t29d","rv6","p33","u68l","cad"};
		DBApp.createTable("q44fp", cols0);
		String tableTrace0 = DBApp.getLastTrace("q44fp");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("q44fp"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test163TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"u4q6y"};
		DBApp.createTable("xxi", cols0);
		String tableTrace0 = DBApp.getLastTrace("xxi");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("xxi"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"wg0","jc","njz","k64q","zc5","p5o","x720h"};
		DBApp.createTable("w02j", cols1);
		String tableTrace1 = DBApp.getLastTrace("w02j");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("w02j"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"t5vr"};
		DBApp.createTable("l68", cols2);
		String tableTrace2 = DBApp.getLastTrace("l68");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("l68"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"z5r","y8u","wt38h","ze8","hs","x0j","gc4"};
		DBApp.createTable("x1", cols3);
		String tableTrace3 = DBApp.getLastTrace("x1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("x1"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"e80b9","h2h","o4","e8","x9","zmtt","n0m"};
		DBApp.createTable("y8t", cols4);
		String tableTrace4 = DBApp.getLastTrace("y8t");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("y8t"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test164TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"pp087"};
		DBApp.createTable("fiu", cols0);
		String tableTrace0 = DBApp.getLastTrace("fiu");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("fiu"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"l2d","jq90v","qx6u","por","t6"};
		DBApp.createTable("aywgp", cols1);
		String tableTrace1 = DBApp.getLastTrace("aywgp");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("aywgp"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"ri","mk5d9","zfbn1","pfi","r3","nys2","ib","fg81","eiewk","jfr"};
		DBApp.createTable("cp4", cols2);
		String tableTrace2 = DBApp.getLastTrace("cp4");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("cp4"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"d39","t3","j9ok","mz","a3l0"};
		DBApp.createTable("g9ok", cols3);
		String tableTrace3 = DBApp.getLastTrace("g9ok");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("g9ok"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"y24z6","h17f","uj","h5x70","s4e7","qx","k4s"};
		DBApp.createTable("ifg", cols4);
		String tableTrace4 = DBApp.getLastTrace("ifg");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("ifg"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test165TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"k07e2"};
		DBApp.createTable("g8uv", cols0);
		String tableTrace0 = DBApp.getLastTrace("g8uv");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("g8uv"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"uy7","m9wi","h93","ohb","s548k","m73","mnt"};
		DBApp.createTable("k6", cols1);
		String tableTrace1 = DBApp.getLastTrace("k6");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("k6"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test166TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"oe0","l4u","hv3"};
		DBApp.createTable("xl6", cols0);
		String tableTrace0 = DBApp.getLastTrace("xl6");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("xl6"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test167TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"hd8c","t6ry6","bm319","jcs6"};
		DBApp.createTable("ovn", cols0);
		String tableTrace0 = DBApp.getLastTrace("ovn");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("ovn"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"hqk7","zd73","f65","b5w","b6b","b16","zn0","h47","qa1y"};
		DBApp.createTable("lw4", cols1);
		String tableTrace1 = DBApp.getLastTrace("lw4");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("lw4"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test168TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"acie","s56n","q2m02","gud","ib","e2"};
		DBApp.createTable("cfurk", cols0);
		String tableTrace0 = DBApp.getLastTrace("cfurk");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("cfurk"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test169TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a9","kw"};
		DBApp.createTable("xg1", cols0);
		String tableTrace0 = DBApp.getLastTrace("xg1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("xg1"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"jhw2r","y6a","l743","a742","lla31","nb3x8"};
		DBApp.createTable("t9", cols1);
		String tableTrace1 = DBApp.getLastTrace("t9");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("t9"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"l12l0","uu","n63p3","itycg","w146","r9n2","ued"};
		DBApp.createTable("w0", cols2);
		String tableTrace2 = DBApp.getLastTrace("w0");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("w0"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"uc3ev","q46hf"};
		DBApp.createTable("br", cols3);
		String tableTrace3 = DBApp.getLastTrace("br");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("br"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"g52bq","j0x","qo6","q0s","yk51","k9","fa5","v9t"};
		DBApp.createTable("a5", cols4);
		String tableTrace4 = DBApp.getLastTrace("a5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("a5"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test0TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"ex","ao2","hibkn","c28j8","be1oc"};
		DBApp.createTable("r89", cols0);
		for(int i=0;i<63;i++)
		{
			String [] record_r89 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_r89[j] = genRandString();
			}
			DBApp.insert("r89", record_r89);
		}
		int pagesCount_r89 = (int)Math.ceil(63.0/DBApp.dataPageSize);
		ArrayList<String> res_r89 = new ArrayList<>();
		for(int i=0;i<pagesCount_r89;i++)
		{
			res_r89.add(i+".db");
		}
		Collections.sort(res_r89);
		String pagesTraceString_r89 = "r89{ " + String.join(" ",res_r89);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_r89+")", fileManagerTrace0.contains(pagesTraceString_r89));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test1TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"of01","e2r4"};
		DBApp.createTable("cl", cols0);
		for(int i=0;i<95;i++)
		{
			String [] record_cl = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_cl[j] = genRandString();
			}
			DBApp.insert("cl", record_cl);
		}
		int pagesCount_cl = (int)Math.ceil(95.0/DBApp.dataPageSize);
		ArrayList<String> res_cl = new ArrayList<>();
		for(int i=0;i<pagesCount_cl;i++)
		{
			res_cl.add(i+".db");
		}
		Collections.sort(res_cl);
		String pagesTraceString_cl = "cl{ " + String.join(" ",res_cl);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_cl+")", fileManagerTrace0.contains(pagesTraceString_cl));
		//--------------------------------------------------------------------------
		String[] cols1 = {"b5","zaq"};
		DBApp.createTable("t7", cols1);
		for(int i=0;i<16;i++)
		{
			String [] record_t7 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_t7[j] = genRandString();
			}
			DBApp.insert("t7", record_t7);
		}
		int pagesCount_t7 = (int)Math.ceil(16.0/DBApp.dataPageSize);
		ArrayList<String> res_t7 = new ArrayList<>();
		for(int i=0;i<pagesCount_t7;i++)
		{
			res_t7.add(i+".db");
		}
		Collections.sort(res_t7);
		String pagesTraceString_t7 = "t7{ " + String.join(" ",res_t7);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_t7+")", fileManagerTrace1.contains(pagesTraceString_t7));
		//--------------------------------------------------------------------------
		String[] cols2 = {"s1q3n","nw"};
		DBApp.createTable("oho", cols2);
		for(int i=0;i<13;i++)
		{
			String [] record_oho = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_oho[j] = genRandString();
			}
			DBApp.insert("oho", record_oho);
		}
		int pagesCount_oho = (int)Math.ceil(13.0/DBApp.dataPageSize);
		ArrayList<String> res_oho = new ArrayList<>();
		for(int i=0;i<pagesCount_oho;i++)
		{
			res_oho.add(i+".db");
		}
		Collections.sort(res_oho);
		String pagesTraceString_oho = "oho{ " + String.join(" ",res_oho);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_oho+")", fileManagerTrace2.contains(pagesTraceString_oho));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a8t","d8"};
		DBApp.createTable("ttdt", cols3);
		for(int i=0;i<25;i++)
		{
			String [] record_ttdt = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_ttdt[j] = genRandString();
			}
			DBApp.insert("ttdt", record_ttdt);
		}
		int pagesCount_ttdt = (int)Math.ceil(25.0/DBApp.dataPageSize);
		ArrayList<String> res_ttdt = new ArrayList<>();
		for(int i=0;i<pagesCount_ttdt;i++)
		{
			res_ttdt.add(i+".db");
		}
		Collections.sort(res_ttdt);
		String pagesTraceString_ttdt = "ttdt{ " + String.join(" ",res_ttdt);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ttdt+")", fileManagerTrace3.contains(pagesTraceString_ttdt));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test2TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"ezhj2","a2","jg","jpj","fdt","kr"};
		DBApp.createTable("ogq9", cols0);
		for(int i=0;i<41;i++)
		{
			String [] record_ogq9 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ogq9[j] = genRandString();
			}
			DBApp.insert("ogq9", record_ogq9);
		}
		int pagesCount_ogq9 = (int)Math.ceil(41.0/DBApp.dataPageSize);
		ArrayList<String> res_ogq9 = new ArrayList<>();
		for(int i=0;i<pagesCount_ogq9;i++)
		{
			res_ogq9.add(i+".db");
		}
		Collections.sort(res_ogq9);
		String pagesTraceString_ogq9 = "ogq9{ " + String.join(" ",res_ogq9);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ogq9+")", fileManagerTrace0.contains(pagesTraceString_ogq9));
		//--------------------------------------------------------------------------
		String[] cols1 = {"s4","d3o9","o2qg8","ue","iv","g3","tmb","mj","w6t"};
		DBApp.createTable("cze", cols1);
		for(int i=0;i<25;i++)
		{
			String [] record_cze = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_cze[j] = genRandString();
			}
			DBApp.insert("cze", record_cze);
		}
		int pagesCount_cze = (int)Math.ceil(25.0/DBApp.dataPageSize);
		ArrayList<String> res_cze = new ArrayList<>();
		for(int i=0;i<pagesCount_cze;i++)
		{
			res_cze.add(i+".db");
		}
		Collections.sort(res_cze);
		String pagesTraceString_cze = "cze{ " + String.join(" ",res_cze);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_cze+")", fileManagerTrace1.contains(pagesTraceString_cze));
		//--------------------------------------------------------------------------
		String[] cols2 = {"f4165","csi","tu","k8r2n"};
		DBApp.createTable("uz", cols2);
		for(int i=0;i<62;i++)
		{
			String [] record_uz = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_uz[j] = genRandString();
			}
			DBApp.insert("uz", record_uz);
		}
		int pagesCount_uz = (int)Math.ceil(62.0/DBApp.dataPageSize);
		ArrayList<String> res_uz = new ArrayList<>();
		for(int i=0;i<pagesCount_uz;i++)
		{
			res_uz.add(i+".db");
		}
		Collections.sort(res_uz);
		String pagesTraceString_uz = "uz{ " + String.join(" ",res_uz);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_uz+")", fileManagerTrace2.contains(pagesTraceString_uz));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test3TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"ea","jlv59","y5","w15l"};
		DBApp.createTable("v5eg1", cols0);
		for(int i=0;i<15;i++)
		{
			String [] record_v5eg1 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_v5eg1[j] = genRandString();
			}
			DBApp.insert("v5eg1", record_v5eg1);
		}
		int pagesCount_v5eg1 = (int)Math.ceil(15.0/DBApp.dataPageSize);
		ArrayList<String> res_v5eg1 = new ArrayList<>();
		for(int i=0;i<pagesCount_v5eg1;i++)
		{
			res_v5eg1.add(i+".db");
		}
		Collections.sort(res_v5eg1);
		String pagesTraceString_v5eg1 = "v5eg1{ " + String.join(" ",res_v5eg1);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_v5eg1+")", fileManagerTrace0.contains(pagesTraceString_v5eg1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"fw5l","x9hg","qy2sm","e70j","gaxav","v5p","uwu14"};
		DBApp.createTable("q96", cols1);
		for(int i=0;i<25;i++)
		{
			String [] record_q96 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_q96[j] = genRandString();
			}
			DBApp.insert("q96", record_q96);
		}
		int pagesCount_q96 = (int)Math.ceil(25.0/DBApp.dataPageSize);
		ArrayList<String> res_q96 = new ArrayList<>();
		for(int i=0;i<pagesCount_q96;i++)
		{
			res_q96.add(i+".db");
		}
		Collections.sort(res_q96);
		String pagesTraceString_q96 = "q96{ " + String.join(" ",res_q96);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_q96+")", fileManagerTrace1.contains(pagesTraceString_q96));
		//--------------------------------------------------------------------------
		String[] cols2 = {"e220","qrlj","j7"};
		DBApp.createTable("em1", cols2);
		for(int i=0;i<10;i++)
		{
			String [] record_em1 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_em1[j] = genRandString();
			}
			DBApp.insert("em1", record_em1);
		}
		int pagesCount_em1 = (int)Math.ceil(10.0/DBApp.dataPageSize);
		ArrayList<String> res_em1 = new ArrayList<>();
		for(int i=0;i<pagesCount_em1;i++)
		{
			res_em1.add(i+".db");
		}
		Collections.sort(res_em1);
		String pagesTraceString_em1 = "em1{ " + String.join(" ",res_em1);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_em1+")", fileManagerTrace2.contains(pagesTraceString_em1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"lm461","o03","lw2"};
		DBApp.createTable("l05qt", cols3);
		for(int i=0;i<40;i++)
		{
			String [] record_l05qt = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_l05qt[j] = genRandString();
			}
			DBApp.insert("l05qt", record_l05qt);
		}
		int pagesCount_l05qt = (int)Math.ceil(40.0/DBApp.dataPageSize);
		ArrayList<String> res_l05qt = new ArrayList<>();
		for(int i=0;i<pagesCount_l05qt;i++)
		{
			res_l05qt.add(i+".db");
		}
		Collections.sort(res_l05qt);
		String pagesTraceString_l05qt = "l05qt{ " + String.join(" ",res_l05qt);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_l05qt+")", fileManagerTrace3.contains(pagesTraceString_l05qt));
		//--------------------------------------------------------------------------
		String[] cols4 = {"rf1","e3u0"};
		DBApp.createTable("jtm", cols4);
		for(int i=0;i<14;i++)
		{
			String [] record_jtm = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_jtm[j] = genRandString();
			}
			DBApp.insert("jtm", record_jtm);
		}
		int pagesCount_jtm = (int)Math.ceil(14.0/DBApp.dataPageSize);
		ArrayList<String> res_jtm = new ArrayList<>();
		for(int i=0;i<pagesCount_jtm;i++)
		{
			res_jtm.add(i+".db");
		}
		Collections.sort(res_jtm);
		String pagesTraceString_jtm = "jtm{ " + String.join(" ",res_jtm);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_jtm+")", fileManagerTrace4.contains(pagesTraceString_jtm));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test4TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"mjo","bfw","xj0ng","t30t","ml0","znzy"};
		DBApp.createTable("k0pbr", cols0);
		for(int i=0;i<52;i++)
		{
			String [] record_k0pbr = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_k0pbr[j] = genRandString();
			}
			DBApp.insert("k0pbr", record_k0pbr);
		}
		int pagesCount_k0pbr = (int)Math.ceil(52.0/DBApp.dataPageSize);
		ArrayList<String> res_k0pbr = new ArrayList<>();
		for(int i=0;i<pagesCount_k0pbr;i++)
		{
			res_k0pbr.add(i+".db");
		}
		Collections.sort(res_k0pbr);
		String pagesTraceString_k0pbr = "k0pbr{ " + String.join(" ",res_k0pbr);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_k0pbr+")", fileManagerTrace0.contains(pagesTraceString_k0pbr));
		//--------------------------------------------------------------------------
		String[] cols1 = {"t2"};
		DBApp.createTable("pyy7", cols1);
		for(int i=0;i<27;i++)
		{
			String [] record_pyy7 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_pyy7[j] = genRandString();
			}
			DBApp.insert("pyy7", record_pyy7);
		}
		int pagesCount_pyy7 = (int)Math.ceil(27.0/DBApp.dataPageSize);
		ArrayList<String> res_pyy7 = new ArrayList<>();
		for(int i=0;i<pagesCount_pyy7;i++)
		{
			res_pyy7.add(i+".db");
		}
		Collections.sort(res_pyy7);
		String pagesTraceString_pyy7 = "pyy7{ " + String.join(" ",res_pyy7);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_pyy7+")", fileManagerTrace1.contains(pagesTraceString_pyy7));
		//--------------------------------------------------------------------------
		String[] cols2 = {"isx0m","u6","vz2","m9ac","gjjdm","z0x","mi66","curf7","bd","f7"};
		DBApp.createTable("p3j", cols2);
		for(int i=0;i<4;i++)
		{
			String [] record_p3j = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_p3j[j] = genRandString();
			}
			DBApp.insert("p3j", record_p3j);
		}
		int pagesCount_p3j = (int)Math.ceil(4.0/DBApp.dataPageSize);
		ArrayList<String> res_p3j = new ArrayList<>();
		for(int i=0;i<pagesCount_p3j;i++)
		{
			res_p3j.add(i+".db");
		}
		Collections.sort(res_p3j);
		String pagesTraceString_p3j = "p3j{ " + String.join(" ",res_p3j);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_p3j+")", fileManagerTrace2.contains(pagesTraceString_p3j));
		//--------------------------------------------------------------------------
		String[] cols3 = {"tj6","iq8","srh","mh5","p20dw","u4","ua","ogv","lr44","ql0a"};
		DBApp.createTable("yv", cols3);
		for(int i=0;i<100;i++)
		{
			String [] record_yv = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_yv[j] = genRandString();
			}
			DBApp.insert("yv", record_yv);
		}
		int pagesCount_yv = (int)Math.ceil(100.0/DBApp.dataPageSize);
		ArrayList<String> res_yv = new ArrayList<>();
		for(int i=0;i<pagesCount_yv;i++)
		{
			res_yv.add(i+".db");
		}
		Collections.sort(res_yv);
		String pagesTraceString_yv = "yv{ " + String.join(" ",res_yv);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_yv+")", fileManagerTrace3.contains(pagesTraceString_yv));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test5TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"n4","yx6","ij8","e0e","sxl","zd","q25i","v0ms2","xhlw","j7p"};
		DBApp.createTable("qy84s", cols0);
		for(int i=0;i<14;i++)
		{
			String [] record_qy84s = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_qy84s[j] = genRandString();
			}
			DBApp.insert("qy84s", record_qy84s);
		}
		int pagesCount_qy84s = (int)Math.ceil(14.0/DBApp.dataPageSize);
		ArrayList<String> res_qy84s = new ArrayList<>();
		for(int i=0;i<pagesCount_qy84s;i++)
		{
			res_qy84s.add(i+".db");
		}
		Collections.sort(res_qy84s);
		String pagesTraceString_qy84s = "qy84s{ " + String.join(" ",res_qy84s);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_qy84s+")", fileManagerTrace0.contains(pagesTraceString_qy84s));
		//--------------------------------------------------------------------------
		String[] cols1 = {"g2s","f91","n4","i30","u3m","ht","x490k","p2j","d9v9"};
		DBApp.createTable("m7", cols1);
		for(int i=0;i<92;i++)
		{
			String [] record_m7 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_m7[j] = genRandString();
			}
			DBApp.insert("m7", record_m7);
		}
		int pagesCount_m7 = (int)Math.ceil(92.0/DBApp.dataPageSize);
		ArrayList<String> res_m7 = new ArrayList<>();
		for(int i=0;i<pagesCount_m7;i++)
		{
			res_m7.add(i+".db");
		}
		Collections.sort(res_m7);
		String pagesTraceString_m7 = "m7{ " + String.join(" ",res_m7);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_m7+")", fileManagerTrace1.contains(pagesTraceString_m7));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test6TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"h8f","v4","d72","l69","wd8","u32f6","q47sl"};
		DBApp.createTable("tbq0", cols0);
		for(int i=0;i<45;i++)
		{
			String [] record_tbq0 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_tbq0[j] = genRandString();
			}
			DBApp.insert("tbq0", record_tbq0);
		}
		int pagesCount_tbq0 = (int)Math.ceil(45.0/DBApp.dataPageSize);
		ArrayList<String> res_tbq0 = new ArrayList<>();
		for(int i=0;i<pagesCount_tbq0;i++)
		{
			res_tbq0.add(i+".db");
		}
		Collections.sort(res_tbq0);
		String pagesTraceString_tbq0 = "tbq0{ " + String.join(" ",res_tbq0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_tbq0+")", fileManagerTrace0.contains(pagesTraceString_tbq0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"c68g1","h7o9y","b7b","ki8","b8y"};
		DBApp.createTable("e81", cols1);
		for(int i=0;i<69;i++)
		{
			String [] record_e81 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_e81[j] = genRandString();
			}
			DBApp.insert("e81", record_e81);
		}
		int pagesCount_e81 = (int)Math.ceil(69.0/DBApp.dataPageSize);
		ArrayList<String> res_e81 = new ArrayList<>();
		for(int i=0;i<pagesCount_e81;i++)
		{
			res_e81.add(i+".db");
		}
		Collections.sort(res_e81);
		String pagesTraceString_e81 = "e81{ " + String.join(" ",res_e81);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_e81+")", fileManagerTrace1.contains(pagesTraceString_e81));
		//--------------------------------------------------------------------------
		String[] cols2 = {"w3f","vi3","w52w","m5","edo","e8g","qmq","zcdf"};
		DBApp.createTable("i0e14", cols2);
		for(int i=0;i<54;i++)
		{
			String [] record_i0e14 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_i0e14[j] = genRandString();
			}
			DBApp.insert("i0e14", record_i0e14);
		}
		int pagesCount_i0e14 = (int)Math.ceil(54.0/DBApp.dataPageSize);
		ArrayList<String> res_i0e14 = new ArrayList<>();
		for(int i=0;i<pagesCount_i0e14;i++)
		{
			res_i0e14.add(i+".db");
		}
		Collections.sort(res_i0e14);
		String pagesTraceString_i0e14 = "i0e14{ " + String.join(" ",res_i0e14);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_i0e14+")", fileManagerTrace2.contains(pagesTraceString_i0e14));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test7TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"uv9l","wn4","dj","q7","q3","e0q","w57q"};
		DBApp.createTable("nuh", cols0);
		for(int i=0;i<66;i++)
		{
			String [] record_nuh = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_nuh[j] = genRandString();
			}
			DBApp.insert("nuh", record_nuh);
		}
		int pagesCount_nuh = (int)Math.ceil(66.0/DBApp.dataPageSize);
		ArrayList<String> res_nuh = new ArrayList<>();
		for(int i=0;i<pagesCount_nuh;i++)
		{
			res_nuh.add(i+".db");
		}
		Collections.sort(res_nuh);
		String pagesTraceString_nuh = "nuh{ " + String.join(" ",res_nuh);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_nuh+")", fileManagerTrace0.contains(pagesTraceString_nuh));
		//--------------------------------------------------------------------------
		String[] cols1 = {"d6g9","y6","x80","tu7","ef81l","x09","yo","n3r","bs7"};
		DBApp.createTable("in5", cols1);
		for(int i=0;i<7;i++)
		{
			String [] record_in5 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_in5[j] = genRandString();
			}
			DBApp.insert("in5", record_in5);
		}
		int pagesCount_in5 = (int)Math.ceil(7.0/DBApp.dataPageSize);
		ArrayList<String> res_in5 = new ArrayList<>();
		for(int i=0;i<pagesCount_in5;i++)
		{
			res_in5.add(i+".db");
		}
		Collections.sort(res_in5);
		String pagesTraceString_in5 = "in5{ " + String.join(" ",res_in5);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_in5+")", fileManagerTrace1.contains(pagesTraceString_in5));
		//--------------------------------------------------------------------------
		String[] cols2 = {"gfw3x","wkd","v52","s0","gexe","p5","n7q","z9g8","e6l0a"};
		DBApp.createTable("q207", cols2);
		for(int i=0;i<5;i++)
		{
			String [] record_q207 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_q207[j] = genRandString();
			}
			DBApp.insert("q207", record_q207);
		}
		int pagesCount_q207 = (int)Math.ceil(5.0/DBApp.dataPageSize);
		ArrayList<String> res_q207 = new ArrayList<>();
		for(int i=0;i<pagesCount_q207;i++)
		{
			res_q207.add(i+".db");
		}
		Collections.sort(res_q207);
		String pagesTraceString_q207 = "q207{ " + String.join(" ",res_q207);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_q207+")", fileManagerTrace2.contains(pagesTraceString_q207));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test8TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"k2hs","j0rq","z653","e96wt","gw9","zt0","e9629","den","mo5s","t48"};
		DBApp.createTable("yw", cols0);
		for(int i=0;i<81;i++)
		{
			String [] record_yw = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_yw[j] = genRandString();
			}
			DBApp.insert("yw", record_yw);
		}
		int pagesCount_yw = (int)Math.ceil(81.0/DBApp.dataPageSize);
		ArrayList<String> res_yw = new ArrayList<>();
		for(int i=0;i<pagesCount_yw;i++)
		{
			res_yw.add(i+".db");
		}
		Collections.sort(res_yw);
		String pagesTraceString_yw = "yw{ " + String.join(" ",res_yw);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_yw+")", fileManagerTrace0.contains(pagesTraceString_yw));
		//--------------------------------------------------------------------------
		String[] cols1 = {"w89qc","s0k","st7r6","g7l","u4h","s7p"};
		DBApp.createTable("dz2", cols1);
		for(int i=0;i<6;i++)
		{
			String [] record_dz2 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_dz2[j] = genRandString();
			}
			DBApp.insert("dz2", record_dz2);
		}
		int pagesCount_dz2 = (int)Math.ceil(6.0/DBApp.dataPageSize);
		ArrayList<String> res_dz2 = new ArrayList<>();
		for(int i=0;i<pagesCount_dz2;i++)
		{
			res_dz2.add(i+".db");
		}
		Collections.sort(res_dz2);
		String pagesTraceString_dz2 = "dz2{ " + String.join(" ",res_dz2);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_dz2+")", fileManagerTrace1.contains(pagesTraceString_dz2));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a6i"};
		DBApp.createTable("cti", cols2);
		for(int i=0;i<7;i++)
		{
			String [] record_cti = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_cti[j] = genRandString();
			}
			DBApp.insert("cti", record_cti);
		}
		int pagesCount_cti = (int)Math.ceil(7.0/DBApp.dataPageSize);
		ArrayList<String> res_cti = new ArrayList<>();
		for(int i=0;i<pagesCount_cti;i++)
		{
			res_cti.add(i+".db");
		}
		Collections.sort(res_cti);
		String pagesTraceString_cti = "cti{ " + String.join(" ",res_cti);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_cti+")", fileManagerTrace2.contains(pagesTraceString_cti));
		//--------------------------------------------------------------------------
		String[] cols3 = {"z5g9"};
		DBApp.createTable("rke", cols3);
		for(int i=0;i<34;i++)
		{
			String [] record_rke = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_rke[j] = genRandString();
			}
			DBApp.insert("rke", record_rke);
		}
		int pagesCount_rke = (int)Math.ceil(34.0/DBApp.dataPageSize);
		ArrayList<String> res_rke = new ArrayList<>();
		for(int i=0;i<pagesCount_rke;i++)
		{
			res_rke.add(i+".db");
		}
		Collections.sort(res_rke);
		String pagesTraceString_rke = "rke{ " + String.join(" ",res_rke);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_rke+")", fileManagerTrace3.contains(pagesTraceString_rke));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test9TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"fta","txj","s7","f2e","ywl","j01","t2i"};
		DBApp.createTable("q45", cols0);
		for(int i=0;i<84;i++)
		{
			String [] record_q45 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_q45[j] = genRandString();
			}
			DBApp.insert("q45", record_q45);
		}
		int pagesCount_q45 = (int)Math.ceil(84.0/DBApp.dataPageSize);
		ArrayList<String> res_q45 = new ArrayList<>();
		for(int i=0;i<pagesCount_q45;i++)
		{
			res_q45.add(i+".db");
		}
		Collections.sort(res_q45);
		String pagesTraceString_q45 = "q45{ " + String.join(" ",res_q45);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_q45+")", fileManagerTrace0.contains(pagesTraceString_q45));
		//--------------------------------------------------------------------------
		String[] cols1 = {"vi6"};
		DBApp.createTable("dz2", cols1);
		for(int i=0;i<83;i++)
		{
			String [] record_dz2 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_dz2[j] = genRandString();
			}
			DBApp.insert("dz2", record_dz2);
		}
		int pagesCount_dz2 = (int)Math.ceil(83.0/DBApp.dataPageSize);
		ArrayList<String> res_dz2 = new ArrayList<>();
		for(int i=0;i<pagesCount_dz2;i++)
		{
			res_dz2.add(i+".db");
		}
		Collections.sort(res_dz2);
		String pagesTraceString_dz2 = "dz2{ " + String.join(" ",res_dz2);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_dz2+")", fileManagerTrace1.contains(pagesTraceString_dz2));
		//--------------------------------------------------------------------------
		String[] cols2 = {"mcx","u042","jd1","dqtp3"};
		DBApp.createTable("k3", cols2);
		for(int i=0;i<17;i++)
		{
			String [] record_k3 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_k3[j] = genRandString();
			}
			DBApp.insert("k3", record_k3);
		}
		int pagesCount_k3 = (int)Math.ceil(17.0/DBApp.dataPageSize);
		ArrayList<String> res_k3 = new ArrayList<>();
		for(int i=0;i<pagesCount_k3;i++)
		{
			res_k3.add(i+".db");
		}
		Collections.sort(res_k3);
		String pagesTraceString_k3 = "k3{ " + String.join(" ",res_k3);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_k3+")", fileManagerTrace2.contains(pagesTraceString_k3));
		//--------------------------------------------------------------------------
		String[] cols3 = {"w86"};
		DBApp.createTable("zyfq", cols3);
		for(int i=0;i<43;i++)
		{
			String [] record_zyfq = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_zyfq[j] = genRandString();
			}
			DBApp.insert("zyfq", record_zyfq);
		}
		int pagesCount_zyfq = (int)Math.ceil(43.0/DBApp.dataPageSize);
		ArrayList<String> res_zyfq = new ArrayList<>();
		for(int i=0;i<pagesCount_zyfq;i++)
		{
			res_zyfq.add(i+".db");
		}
		Collections.sort(res_zyfq);
		String pagesTraceString_zyfq = "zyfq{ " + String.join(" ",res_zyfq);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_zyfq+")", fileManagerTrace3.contains(pagesTraceString_zyfq));
		//--------------------------------------------------------------------------
		String[] cols4 = {"t0lo"};
		DBApp.createTable("zw", cols4);
		for(int i=0;i<95;i++)
		{
			String [] record_zw = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_zw[j] = genRandString();
			}
			DBApp.insert("zw", record_zw);
		}
		int pagesCount_zw = (int)Math.ceil(95.0/DBApp.dataPageSize);
		ArrayList<String> res_zw = new ArrayList<>();
		for(int i=0;i<pagesCount_zw;i++)
		{
			res_zw.add(i+".db");
		}
		Collections.sort(res_zw);
		String pagesTraceString_zw = "zw{ " + String.join(" ",res_zw);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_zw+")", fileManagerTrace4.contains(pagesTraceString_zw));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test10TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"n4","y9u","yk6w","p6y45","bo"};
		DBApp.createTable("jiqu9", cols0);
		for(int i=0;i<18;i++)
		{
			String [] record_jiqu9 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_jiqu9[j] = genRandString();
			}
			DBApp.insert("jiqu9", record_jiqu9);
		}
		int pagesCount_jiqu9 = (int)Math.ceil(18.0/DBApp.dataPageSize);
		ArrayList<String> res_jiqu9 = new ArrayList<>();
		for(int i=0;i<pagesCount_jiqu9;i++)
		{
			res_jiqu9.add(i+".db");
		}
		Collections.sort(res_jiqu9);
		String pagesTraceString_jiqu9 = "jiqu9{ " + String.join(" ",res_jiqu9);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_jiqu9+")", fileManagerTrace0.contains(pagesTraceString_jiqu9));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test11TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"oofo2","qun","oi1","lq","wjd","q0d","ml7t"};
		DBApp.createTable("n879", cols0);
		for(int i=0;i<74;i++)
		{
			String [] record_n879 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_n879[j] = genRandString();
			}
			DBApp.insert("n879", record_n879);
		}
		int pagesCount_n879 = (int)Math.ceil(74.0/DBApp.dataPageSize);
		ArrayList<String> res_n879 = new ArrayList<>();
		for(int i=0;i<pagesCount_n879;i++)
		{
			res_n879.add(i+".db");
		}
		Collections.sort(res_n879);
		String pagesTraceString_n879 = "n879{ " + String.join(" ",res_n879);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_n879+")", fileManagerTrace0.contains(pagesTraceString_n879));
		//--------------------------------------------------------------------------
		String[] cols1 = {"vw","w65","psj"};
		DBApp.createTable("hsr", cols1);
		for(int i=0;i<3;i++)
		{
			String [] record_hsr = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_hsr[j] = genRandString();
			}
			DBApp.insert("hsr", record_hsr);
		}
		int pagesCount_hsr = (int)Math.ceil(3.0/DBApp.dataPageSize);
		ArrayList<String> res_hsr = new ArrayList<>();
		for(int i=0;i<pagesCount_hsr;i++)
		{
			res_hsr.add(i+".db");
		}
		Collections.sort(res_hsr);
		String pagesTraceString_hsr = "hsr{ " + String.join(" ",res_hsr);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_hsr+")", fileManagerTrace1.contains(pagesTraceString_hsr));
		//--------------------------------------------------------------------------
		String[] cols2 = {"csq7v","lnz6","vd","a95f","q80","tik"};
		DBApp.createTable("j6", cols2);
		for(int i=0;i<30;i++)
		{
			String [] record_j6 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_j6[j] = genRandString();
			}
			DBApp.insert("j6", record_j6);
		}
		int pagesCount_j6 = (int)Math.ceil(30.0/DBApp.dataPageSize);
		ArrayList<String> res_j6 = new ArrayList<>();
		for(int i=0;i<pagesCount_j6;i++)
		{
			res_j6.add(i+".db");
		}
		Collections.sort(res_j6);
		String pagesTraceString_j6 = "j6{ " + String.join(" ",res_j6);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_j6+")", fileManagerTrace2.contains(pagesTraceString_j6));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test12TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"th","a354","p73p6","nh","l6","i56t3"};
		DBApp.createTable("r28", cols0);
		for(int i=0;i<4;i++)
		{
			String [] record_r28 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_r28[j] = genRandString();
			}
			DBApp.insert("r28", record_r28);
		}
		int pagesCount_r28 = (int)Math.ceil(4.0/DBApp.dataPageSize);
		ArrayList<String> res_r28 = new ArrayList<>();
		for(int i=0;i<pagesCount_r28;i++)
		{
			res_r28.add(i+".db");
		}
		Collections.sort(res_r28);
		String pagesTraceString_r28 = "r28{ " + String.join(" ",res_r28);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_r28+")", fileManagerTrace0.contains(pagesTraceString_r28));
		//--------------------------------------------------------------------------
		String[] cols1 = {"t9w7","f1k92","jqp6","eo","dbqs9"};
		DBApp.createTable("e5xw", cols1);
		for(int i=0;i<70;i++)
		{
			String [] record_e5xw = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_e5xw[j] = genRandString();
			}
			DBApp.insert("e5xw", record_e5xw);
		}
		int pagesCount_e5xw = (int)Math.ceil(70.0/DBApp.dataPageSize);
		ArrayList<String> res_e5xw = new ArrayList<>();
		for(int i=0;i<pagesCount_e5xw;i++)
		{
			res_e5xw.add(i+".db");
		}
		Collections.sort(res_e5xw);
		String pagesTraceString_e5xw = "e5xw{ " + String.join(" ",res_e5xw);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_e5xw+")", fileManagerTrace1.contains(pagesTraceString_e5xw));
		//--------------------------------------------------------------------------
		String[] cols2 = {"vb2","wi","m6","ik9","ej","u8u84"};
		DBApp.createTable("itf", cols2);
		for(int i=0;i<59;i++)
		{
			String [] record_itf = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_itf[j] = genRandString();
			}
			DBApp.insert("itf", record_itf);
		}
		int pagesCount_itf = (int)Math.ceil(59.0/DBApp.dataPageSize);
		ArrayList<String> res_itf = new ArrayList<>();
		for(int i=0;i<pagesCount_itf;i++)
		{
			res_itf.add(i+".db");
		}
		Collections.sort(res_itf);
		String pagesTraceString_itf = "itf{ " + String.join(" ",res_itf);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_itf+")", fileManagerTrace2.contains(pagesTraceString_itf));
		//--------------------------------------------------------------------------
		String[] cols3 = {"f7","ia8"};
		DBApp.createTable("imn", cols3);
		for(int i=0;i<54;i++)
		{
			String [] record_imn = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_imn[j] = genRandString();
			}
			DBApp.insert("imn", record_imn);
		}
		int pagesCount_imn = (int)Math.ceil(54.0/DBApp.dataPageSize);
		ArrayList<String> res_imn = new ArrayList<>();
		for(int i=0;i<pagesCount_imn;i++)
		{
			res_imn.add(i+".db");
		}
		Collections.sort(res_imn);
		String pagesTraceString_imn = "imn{ " + String.join(" ",res_imn);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_imn+")", fileManagerTrace3.contains(pagesTraceString_imn));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test13TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"kr9","z822o","nt928","jy81","z1fw"};
		DBApp.createTable("p097", cols0);
		for(int i=0;i<5;i++)
		{
			String [] record_p097 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_p097[j] = genRandString();
			}
			DBApp.insert("p097", record_p097);
		}
		int pagesCount_p097 = (int)Math.ceil(5.0/DBApp.dataPageSize);
		ArrayList<String> res_p097 = new ArrayList<>();
		for(int i=0;i<pagesCount_p097;i++)
		{
			res_p097.add(i+".db");
		}
		Collections.sort(res_p097);
		String pagesTraceString_p097 = "p097{ " + String.join(" ",res_p097);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_p097+")", fileManagerTrace0.contains(pagesTraceString_p097));
		//--------------------------------------------------------------------------
		String[] cols1 = {"i7n1v","y38","nc6","tv5","h2y","k4","oo"};
		DBApp.createTable("du7c", cols1);
		for(int i=0;i<22;i++)
		{
			String [] record_du7c = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_du7c[j] = genRandString();
			}
			DBApp.insert("du7c", record_du7c);
		}
		int pagesCount_du7c = (int)Math.ceil(22.0/DBApp.dataPageSize);
		ArrayList<String> res_du7c = new ArrayList<>();
		for(int i=0;i<pagesCount_du7c;i++)
		{
			res_du7c.add(i+".db");
		}
		Collections.sort(res_du7c);
		String pagesTraceString_du7c = "du7c{ " + String.join(" ",res_du7c);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_du7c+")", fileManagerTrace1.contains(pagesTraceString_du7c));
		//--------------------------------------------------------------------------
		String[] cols2 = {"o5a","a2553","ks","v1o9"};
		DBApp.createTable("tkl", cols2);
		for(int i=0;i<50;i++)
		{
			String [] record_tkl = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_tkl[j] = genRandString();
			}
			DBApp.insert("tkl", record_tkl);
		}
		int pagesCount_tkl = (int)Math.ceil(50.0/DBApp.dataPageSize);
		ArrayList<String> res_tkl = new ArrayList<>();
		for(int i=0;i<pagesCount_tkl;i++)
		{
			res_tkl.add(i+".db");
		}
		Collections.sort(res_tkl);
		String pagesTraceString_tkl = "tkl{ " + String.join(" ",res_tkl);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_tkl+")", fileManagerTrace2.contains(pagesTraceString_tkl));
		//--------------------------------------------------------------------------
		String[] cols3 = {"rq9","f45s","xz","sm59","vw4","r66","l8a","oon","y19"};
		DBApp.createTable("w988", cols3);
		for(int i=0;i<40;i++)
		{
			String [] record_w988 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_w988[j] = genRandString();
			}
			DBApp.insert("w988", record_w988);
		}
		int pagesCount_w988 = (int)Math.ceil(40.0/DBApp.dataPageSize);
		ArrayList<String> res_w988 = new ArrayList<>();
		for(int i=0;i<pagesCount_w988;i++)
		{
			res_w988.add(i+".db");
		}
		Collections.sort(res_w988);
		String pagesTraceString_w988 = "w988{ " + String.join(" ",res_w988);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_w988+")", fileManagerTrace3.contains(pagesTraceString_w988));
		//--------------------------------------------------------------------------
		String[] cols4 = {"g9p8","ii49d","y0y","oe","yyx7","t58","me0","pmd","i2","lcv"};
		DBApp.createTable("yph4", cols4);
		for(int i=0;i<37;i++)
		{
			String [] record_yph4 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_yph4[j] = genRandString();
			}
			DBApp.insert("yph4", record_yph4);
		}
		int pagesCount_yph4 = (int)Math.ceil(37.0/DBApp.dataPageSize);
		ArrayList<String> res_yph4 = new ArrayList<>();
		for(int i=0;i<pagesCount_yph4;i++)
		{
			res_yph4.add(i+".db");
		}
		Collections.sort(res_yph4);
		String pagesTraceString_yph4 = "yph4{ " + String.join(" ",res_yph4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_yph4+")", fileManagerTrace4.contains(pagesTraceString_yph4));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test14TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"q8","is","q1690","f2n","fef","v1"};
		DBApp.createTable("m16k4", cols0);
		for(int i=0;i<14;i++)
		{
			String [] record_m16k4 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_m16k4[j] = genRandString();
			}
			DBApp.insert("m16k4", record_m16k4);
		}
		int pagesCount_m16k4 = (int)Math.ceil(14.0/DBApp.dataPageSize);
		ArrayList<String> res_m16k4 = new ArrayList<>();
		for(int i=0;i<pagesCount_m16k4;i++)
		{
			res_m16k4.add(i+".db");
		}
		Collections.sort(res_m16k4);
		String pagesTraceString_m16k4 = "m16k4{ " + String.join(" ",res_m16k4);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_m16k4+")", fileManagerTrace0.contains(pagesTraceString_m16k4));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test15TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"s6n","b7","fu2","t18","j098","utp"};
		DBApp.createTable("ae", cols0);
		for(int i=0;i<7;i++)
		{
			String [] record_ae = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ae[j] = genRandString();
			}
			DBApp.insert("ae", record_ae);
		}
		int pagesCount_ae = (int)Math.ceil(7.0/DBApp.dataPageSize);
		ArrayList<String> res_ae = new ArrayList<>();
		for(int i=0;i<pagesCount_ae;i++)
		{
			res_ae.add(i+".db");
		}
		Collections.sort(res_ae);
		String pagesTraceString_ae = "ae{ " + String.join(" ",res_ae);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ae+")", fileManagerTrace0.contains(pagesTraceString_ae));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a8","dz","b6n7p","um","ml9rm","m9wlf","w4m93","j95"};
		DBApp.createTable("x9u1", cols1);
		for(int i=0;i<73;i++)
		{
			String [] record_x9u1 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_x9u1[j] = genRandString();
			}
			DBApp.insert("x9u1", record_x9u1);
		}
		int pagesCount_x9u1 = (int)Math.ceil(73.0/DBApp.dataPageSize);
		ArrayList<String> res_x9u1 = new ArrayList<>();
		for(int i=0;i<pagesCount_x9u1;i++)
		{
			res_x9u1.add(i+".db");
		}
		Collections.sort(res_x9u1);
		String pagesTraceString_x9u1 = "x9u1{ " + String.join(" ",res_x9u1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_x9u1+")", fileManagerTrace1.contains(pagesTraceString_x9u1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"g2x2","h5j","s81","ou1","i31d"};
		DBApp.createTable("ko0j", cols2);
		for(int i=0;i<82;i++)
		{
			String [] record_ko0j = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_ko0j[j] = genRandString();
			}
			DBApp.insert("ko0j", record_ko0j);
		}
		int pagesCount_ko0j = (int)Math.ceil(82.0/DBApp.dataPageSize);
		ArrayList<String> res_ko0j = new ArrayList<>();
		for(int i=0;i<pagesCount_ko0j;i++)
		{
			res_ko0j.add(i+".db");
		}
		Collections.sort(res_ko0j);
		String pagesTraceString_ko0j = "ko0j{ " + String.join(" ",res_ko0j);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ko0j+")", fileManagerTrace2.contains(pagesTraceString_ko0j));
		//--------------------------------------------------------------------------
		String[] cols3 = {"l83","w7","rc","es37b","n83a5","x7rj"};
		DBApp.createTable("b0d5f", cols3);
		for(int i=0;i<16;i++)
		{
			String [] record_b0d5f = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_b0d5f[j] = genRandString();
			}
			DBApp.insert("b0d5f", record_b0d5f);
		}
		int pagesCount_b0d5f = (int)Math.ceil(16.0/DBApp.dataPageSize);
		ArrayList<String> res_b0d5f = new ArrayList<>();
		for(int i=0;i<pagesCount_b0d5f;i++)
		{
			res_b0d5f.add(i+".db");
		}
		Collections.sort(res_b0d5f);
		String pagesTraceString_b0d5f = "b0d5f{ " + String.join(" ",res_b0d5f);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_b0d5f+")", fileManagerTrace3.contains(pagesTraceString_b0d5f));
		//--------------------------------------------------------------------------
		String[] cols4 = {"g17","s9h"};
		DBApp.createTable("x9i", cols4);
		for(int i=0;i<12;i++)
		{
			String [] record_x9i = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_x9i[j] = genRandString();
			}
			DBApp.insert("x9i", record_x9i);
		}
		int pagesCount_x9i = (int)Math.ceil(12.0/DBApp.dataPageSize);
		ArrayList<String> res_x9i = new ArrayList<>();
		for(int i=0;i<pagesCount_x9i;i++)
		{
			res_x9i.add(i+".db");
		}
		Collections.sort(res_x9i);
		String pagesTraceString_x9i = "x9i{ " + String.join(" ",res_x9i);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_x9i+")", fileManagerTrace4.contains(pagesTraceString_x9i));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test16TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"bhz1","m0h","rrr","xw0r","u0r4"};
		DBApp.createTable("y36x", cols0);
		for(int i=0;i<100;i++)
		{
			String [] record_y36x = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_y36x[j] = genRandString();
			}
			DBApp.insert("y36x", record_y36x);
		}
		int pagesCount_y36x = (int)Math.ceil(100.0/DBApp.dataPageSize);
		ArrayList<String> res_y36x = new ArrayList<>();
		for(int i=0;i<pagesCount_y36x;i++)
		{
			res_y36x.add(i+".db");
		}
		Collections.sort(res_y36x);
		String pagesTraceString_y36x = "y36x{ " + String.join(" ",res_y36x);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_y36x+")", fileManagerTrace0.contains(pagesTraceString_y36x));
		//--------------------------------------------------------------------------
		String[] cols1 = {"dtdm","m6lvd","b9rhy"};
		DBApp.createTable("wpuc", cols1);
		for(int i=0;i<39;i++)
		{
			String [] record_wpuc = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_wpuc[j] = genRandString();
			}
			DBApp.insert("wpuc", record_wpuc);
		}
		int pagesCount_wpuc = (int)Math.ceil(39.0/DBApp.dataPageSize);
		ArrayList<String> res_wpuc = new ArrayList<>();
		for(int i=0;i<pagesCount_wpuc;i++)
		{
			res_wpuc.add(i+".db");
		}
		Collections.sort(res_wpuc);
		String pagesTraceString_wpuc = "wpuc{ " + String.join(" ",res_wpuc);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_wpuc+")", fileManagerTrace1.contains(pagesTraceString_wpuc));
		//--------------------------------------------------------------------------
		String[] cols2 = {"z66t","m72"};
		DBApp.createTable("c7", cols2);
		for(int i=0;i<42;i++)
		{
			String [] record_c7 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_c7[j] = genRandString();
			}
			DBApp.insert("c7", record_c7);
		}
		int pagesCount_c7 = (int)Math.ceil(42.0/DBApp.dataPageSize);
		ArrayList<String> res_c7 = new ArrayList<>();
		for(int i=0;i<pagesCount_c7;i++)
		{
			res_c7.add(i+".db");
		}
		Collections.sort(res_c7);
		String pagesTraceString_c7 = "c7{ " + String.join(" ",res_c7);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_c7+")", fileManagerTrace2.contains(pagesTraceString_c7));
		//--------------------------------------------------------------------------
		String[] cols3 = {"u3q6s","m10g","j9v","f94","syyv","vc78","u16r","wa8","ps","g7bg5"};
		DBApp.createTable("tat", cols3);
		for(int i=0;i<84;i++)
		{
			String [] record_tat = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_tat[j] = genRandString();
			}
			DBApp.insert("tat", record_tat);
		}
		int pagesCount_tat = (int)Math.ceil(84.0/DBApp.dataPageSize);
		ArrayList<String> res_tat = new ArrayList<>();
		for(int i=0;i<pagesCount_tat;i++)
		{
			res_tat.add(i+".db");
		}
		Collections.sort(res_tat);
		String pagesTraceString_tat = "tat{ " + String.join(" ",res_tat);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_tat+")", fileManagerTrace3.contains(pagesTraceString_tat));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test17TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"v4l6"};
		DBApp.createTable("pt1k", cols0);
		for(int i=0;i<41;i++)
		{
			String [] record_pt1k = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_pt1k[j] = genRandString();
			}
			DBApp.insert("pt1k", record_pt1k);
		}
		int pagesCount_pt1k = (int)Math.ceil(41.0/DBApp.dataPageSize);
		ArrayList<String> res_pt1k = new ArrayList<>();
		for(int i=0;i<pagesCount_pt1k;i++)
		{
			res_pt1k.add(i+".db");
		}
		Collections.sort(res_pt1k);
		String pagesTraceString_pt1k = "pt1k{ " + String.join(" ",res_pt1k);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_pt1k+")", fileManagerTrace0.contains(pagesTraceString_pt1k));
		//--------------------------------------------------------------------------
		String[] cols1 = {"l1","o83","y9","bb8","g05"};
		DBApp.createTable("h5tp", cols1);
		for(int i=0;i<29;i++)
		{
			String [] record_h5tp = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_h5tp[j] = genRandString();
			}
			DBApp.insert("h5tp", record_h5tp);
		}
		int pagesCount_h5tp = (int)Math.ceil(29.0/DBApp.dataPageSize);
		ArrayList<String> res_h5tp = new ArrayList<>();
		for(int i=0;i<pagesCount_h5tp;i++)
		{
			res_h5tp.add(i+".db");
		}
		Collections.sort(res_h5tp);
		String pagesTraceString_h5tp = "h5tp{ " + String.join(" ",res_h5tp);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_h5tp+")", fileManagerTrace1.contains(pagesTraceString_h5tp));
		//--------------------------------------------------------------------------
		String[] cols2 = {"wt2cy","y7a","q2","lkja","u24","fzwn"};
		DBApp.createTable("t9w", cols2);
		for(int i=0;i<42;i++)
		{
			String [] record_t9w = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_t9w[j] = genRandString();
			}
			DBApp.insert("t9w", record_t9w);
		}
		int pagesCount_t9w = (int)Math.ceil(42.0/DBApp.dataPageSize);
		ArrayList<String> res_t9w = new ArrayList<>();
		for(int i=0;i<pagesCount_t9w;i++)
		{
			res_t9w.add(i+".db");
		}
		Collections.sort(res_t9w);
		String pagesTraceString_t9w = "t9w{ " + String.join(" ",res_t9w);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_t9w+")", fileManagerTrace2.contains(pagesTraceString_t9w));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a3d"};
		DBApp.createTable("cw0", cols3);
		for(int i=0;i<15;i++)
		{
			String [] record_cw0 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_cw0[j] = genRandString();
			}
			DBApp.insert("cw0", record_cw0);
		}
		int pagesCount_cw0 = (int)Math.ceil(15.0/DBApp.dataPageSize);
		ArrayList<String> res_cw0 = new ArrayList<>();
		for(int i=0;i<pagesCount_cw0;i++)
		{
			res_cw0.add(i+".db");
		}
		Collections.sort(res_cw0);
		String pagesTraceString_cw0 = "cw0{ " + String.join(" ",res_cw0);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_cw0+")", fileManagerTrace3.contains(pagesTraceString_cw0));
		//--------------------------------------------------------------------------
		String[] cols4 = {"q15","grk","m0","vht","g94a8","p459","af3nz","le0","mmt"};
		DBApp.createTable("b61", cols4);
		for(int i=0;i<39;i++)
		{
			String [] record_b61 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_b61[j] = genRandString();
			}
			DBApp.insert("b61", record_b61);
		}
		int pagesCount_b61 = (int)Math.ceil(39.0/DBApp.dataPageSize);
		ArrayList<String> res_b61 = new ArrayList<>();
		for(int i=0;i<pagesCount_b61;i++)
		{
			res_b61.add(i+".db");
		}
		Collections.sort(res_b61);
		String pagesTraceString_b61 = "b61{ " + String.join(" ",res_b61);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_b61+")", fileManagerTrace4.contains(pagesTraceString_b61));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test18TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"n0y4u","tk","qy","j55","pclt9","keo","or6","x57f"};
		DBApp.createTable("w80at", cols0);
		for(int i=0;i<31;i++)
		{
			String [] record_w80at = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_w80at[j] = genRandString();
			}
			DBApp.insert("w80at", record_w80at);
		}
		int pagesCount_w80at = (int)Math.ceil(31.0/DBApp.dataPageSize);
		ArrayList<String> res_w80at = new ArrayList<>();
		for(int i=0;i<pagesCount_w80at;i++)
		{
			res_w80at.add(i+".db");
		}
		Collections.sort(res_w80at);
		String pagesTraceString_w80at = "w80at{ " + String.join(" ",res_w80at);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_w80at+")", fileManagerTrace0.contains(pagesTraceString_w80at));
		//--------------------------------------------------------------------------
		String[] cols1 = {"j9","ge62o","fs22x","u02","ur3","r05","d4rqc","e9r1","o57"};
		DBApp.createTable("rees", cols1);
		for(int i=0;i<19;i++)
		{
			String [] record_rees = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_rees[j] = genRandString();
			}
			DBApp.insert("rees", record_rees);
		}
		int pagesCount_rees = (int)Math.ceil(19.0/DBApp.dataPageSize);
		ArrayList<String> res_rees = new ArrayList<>();
		for(int i=0;i<pagesCount_rees;i++)
		{
			res_rees.add(i+".db");
		}
		Collections.sort(res_rees);
		String pagesTraceString_rees = "rees{ " + String.join(" ",res_rees);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_rees+")", fileManagerTrace1.contains(pagesTraceString_rees));
		//--------------------------------------------------------------------------
		String[] cols2 = {"tlw1","m2","v3"};
		DBApp.createTable("vr83", cols2);
		for(int i=0;i<27;i++)
		{
			String [] record_vr83 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_vr83[j] = genRandString();
			}
			DBApp.insert("vr83", record_vr83);
		}
		int pagesCount_vr83 = (int)Math.ceil(27.0/DBApp.dataPageSize);
		ArrayList<String> res_vr83 = new ArrayList<>();
		for(int i=0;i<pagesCount_vr83;i++)
		{
			res_vr83.add(i+".db");
		}
		Collections.sort(res_vr83);
		String pagesTraceString_vr83 = "vr83{ " + String.join(" ",res_vr83);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_vr83+")", fileManagerTrace2.contains(pagesTraceString_vr83));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test19TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"dv3","or9","d8","l73","g68d","xhe2v"};
		DBApp.createTable("k1", cols0);
		for(int i=0;i<55;i++)
		{
			String [] record_k1 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_k1[j] = genRandString();
			}
			DBApp.insert("k1", record_k1);
		}
		int pagesCount_k1 = (int)Math.ceil(55.0/DBApp.dataPageSize);
		ArrayList<String> res_k1 = new ArrayList<>();
		for(int i=0;i<pagesCount_k1;i++)
		{
			res_k1.add(i+".db");
		}
		Collections.sort(res_k1);
		String pagesTraceString_k1 = "k1{ " + String.join(" ",res_k1);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_k1+")", fileManagerTrace0.contains(pagesTraceString_k1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"qk4z"};
		DBApp.createTable("c51", cols1);
		for(int i=0;i<94;i++)
		{
			String [] record_c51 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_c51[j] = genRandString();
			}
			DBApp.insert("c51", record_c51);
		}
		int pagesCount_c51 = (int)Math.ceil(94.0/DBApp.dataPageSize);
		ArrayList<String> res_c51 = new ArrayList<>();
		for(int i=0;i<pagesCount_c51;i++)
		{
			res_c51.add(i+".db");
		}
		Collections.sort(res_c51);
		String pagesTraceString_c51 = "c51{ " + String.join(" ",res_c51);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_c51+")", fileManagerTrace1.contains(pagesTraceString_c51));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test20TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"s4","qm6","d2r58"};
		DBApp.createTable("g8kw", cols0);
		for(int i=0;i<17;i++)
		{
			String [] record_g8kw = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_g8kw[j] = genRandString();
			}
			DBApp.insert("g8kw", record_g8kw);
		}
		int pagesCount_g8kw = (int)Math.ceil(17.0/DBApp.dataPageSize);
		ArrayList<String> res_g8kw = new ArrayList<>();
		for(int i=0;i<pagesCount_g8kw;i++)
		{
			res_g8kw.add(i+".db");
		}
		Collections.sort(res_g8kw);
		String pagesTraceString_g8kw = "g8kw{ " + String.join(" ",res_g8kw);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_g8kw+")", fileManagerTrace0.contains(pagesTraceString_g8kw));
		//--------------------------------------------------------------------------
		String[] cols1 = {"fwwg5","u8i","t1lh6","l7","vd4"};
		DBApp.createTable("cnv0", cols1);
		for(int i=0;i<55;i++)
		{
			String [] record_cnv0 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_cnv0[j] = genRandString();
			}
			DBApp.insert("cnv0", record_cnv0);
		}
		int pagesCount_cnv0 = (int)Math.ceil(55.0/DBApp.dataPageSize);
		ArrayList<String> res_cnv0 = new ArrayList<>();
		for(int i=0;i<pagesCount_cnv0;i++)
		{
			res_cnv0.add(i+".db");
		}
		Collections.sort(res_cnv0);
		String pagesTraceString_cnv0 = "cnv0{ " + String.join(" ",res_cnv0);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_cnv0+")", fileManagerTrace1.contains(pagesTraceString_cnv0));
		//--------------------------------------------------------------------------
		String[] cols2 = {"o7","ig7a","dr","qny","xn4p","e8k"};
		DBApp.createTable("zdv", cols2);
		for(int i=0;i<53;i++)
		{
			String [] record_zdv = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_zdv[j] = genRandString();
			}
			DBApp.insert("zdv", record_zdv);
		}
		int pagesCount_zdv = (int)Math.ceil(53.0/DBApp.dataPageSize);
		ArrayList<String> res_zdv = new ArrayList<>();
		for(int i=0;i<pagesCount_zdv;i++)
		{
			res_zdv.add(i+".db");
		}
		Collections.sort(res_zdv);
		String pagesTraceString_zdv = "zdv{ " + String.join(" ",res_zdv);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_zdv+")", fileManagerTrace2.contains(pagesTraceString_zdv));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test21TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"uo","w16uh","jf","fn685","oj5","r58j3","tilu6"};
		DBApp.createTable("p2m99", cols0);
		for(int i=0;i<54;i++)
		{
			String [] record_p2m99 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_p2m99[j] = genRandString();
			}
			DBApp.insert("p2m99", record_p2m99);
		}
		int pagesCount_p2m99 = (int)Math.ceil(54.0/DBApp.dataPageSize);
		ArrayList<String> res_p2m99 = new ArrayList<>();
		for(int i=0;i<pagesCount_p2m99;i++)
		{
			res_p2m99.add(i+".db");
		}
		Collections.sort(res_p2m99);
		String pagesTraceString_p2m99 = "p2m99{ " + String.join(" ",res_p2m99);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_p2m99+")", fileManagerTrace0.contains(pagesTraceString_p2m99));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test22TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"i8w","x25","s0xs2","k68","jn29","kas","kg73","in3"};
		DBApp.createTable("auo", cols0);
		for(int i=0;i<20;i++)
		{
			String [] record_auo = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_auo[j] = genRandString();
			}
			DBApp.insert("auo", record_auo);
		}
		int pagesCount_auo = (int)Math.ceil(20.0/DBApp.dataPageSize);
		ArrayList<String> res_auo = new ArrayList<>();
		for(int i=0;i<pagesCount_auo;i++)
		{
			res_auo.add(i+".db");
		}
		Collections.sort(res_auo);
		String pagesTraceString_auo = "auo{ " + String.join(" ",res_auo);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_auo+")", fileManagerTrace0.contains(pagesTraceString_auo));
		//--------------------------------------------------------------------------
		String[] cols1 = {"b7h6l"};
		DBApp.createTable("j1fe4", cols1);
		for(int i=0;i<89;i++)
		{
			String [] record_j1fe4 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_j1fe4[j] = genRandString();
			}
			DBApp.insert("j1fe4", record_j1fe4);
		}
		int pagesCount_j1fe4 = (int)Math.ceil(89.0/DBApp.dataPageSize);
		ArrayList<String> res_j1fe4 = new ArrayList<>();
		for(int i=0;i<pagesCount_j1fe4;i++)
		{
			res_j1fe4.add(i+".db");
		}
		Collections.sort(res_j1fe4);
		String pagesTraceString_j1fe4 = "j1fe4{ " + String.join(" ",res_j1fe4);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_j1fe4+")", fileManagerTrace1.contains(pagesTraceString_j1fe4));
		//--------------------------------------------------------------------------
		String[] cols2 = {"dj","m5g","w2s1f","p0e","vvp","xvj"};
		DBApp.createTable("pev", cols2);
		for(int i=0;i<65;i++)
		{
			String [] record_pev = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_pev[j] = genRandString();
			}
			DBApp.insert("pev", record_pev);
		}
		int pagesCount_pev = (int)Math.ceil(65.0/DBApp.dataPageSize);
		ArrayList<String> res_pev = new ArrayList<>();
		for(int i=0;i<pagesCount_pev;i++)
		{
			res_pev.add(i+".db");
		}
		Collections.sort(res_pev);
		String pagesTraceString_pev = "pev{ " + String.join(" ",res_pev);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_pev+")", fileManagerTrace2.contains(pagesTraceString_pev));
		//--------------------------------------------------------------------------
		String[] cols3 = {"s2m","vn","j738"};
		DBApp.createTable("kpdb", cols3);
		for(int i=0;i<76;i++)
		{
			String [] record_kpdb = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_kpdb[j] = genRandString();
			}
			DBApp.insert("kpdb", record_kpdb);
		}
		int pagesCount_kpdb = (int)Math.ceil(76.0/DBApp.dataPageSize);
		ArrayList<String> res_kpdb = new ArrayList<>();
		for(int i=0;i<pagesCount_kpdb;i++)
		{
			res_kpdb.add(i+".db");
		}
		Collections.sort(res_kpdb);
		String pagesTraceString_kpdb = "kpdb{ " + String.join(" ",res_kpdb);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_kpdb+")", fileManagerTrace3.contains(pagesTraceString_kpdb));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test23TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"xd","iu8d","esi","c26w6","mfl8j"};
		DBApp.createTable("uyv3y", cols0);
		for(int i=0;i<14;i++)
		{
			String [] record_uyv3y = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_uyv3y[j] = genRandString();
			}
			DBApp.insert("uyv3y", record_uyv3y);
		}
		int pagesCount_uyv3y = (int)Math.ceil(14.0/DBApp.dataPageSize);
		ArrayList<String> res_uyv3y = new ArrayList<>();
		for(int i=0;i<pagesCount_uyv3y;i++)
		{
			res_uyv3y.add(i+".db");
		}
		Collections.sort(res_uyv3y);
		String pagesTraceString_uyv3y = "uyv3y{ " + String.join(" ",res_uyv3y);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_uyv3y+")", fileManagerTrace0.contains(pagesTraceString_uyv3y));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test24TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"u09","a0","kmmi4","bl1m","c3"};
		DBApp.createTable("rs859", cols0);
		for(int i=0;i<33;i++)
		{
			String [] record_rs859 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_rs859[j] = genRandString();
			}
			DBApp.insert("rs859", record_rs859);
		}
		int pagesCount_rs859 = (int)Math.ceil(33.0/DBApp.dataPageSize);
		ArrayList<String> res_rs859 = new ArrayList<>();
		for(int i=0;i<pagesCount_rs859;i++)
		{
			res_rs859.add(i+".db");
		}
		Collections.sort(res_rs859);
		String pagesTraceString_rs859 = "rs859{ " + String.join(" ",res_rs859);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_rs859+")", fileManagerTrace0.contains(pagesTraceString_rs859));
		//--------------------------------------------------------------------------
		String[] cols1 = {"g63rc","tl7w","v61","d436","en5"};
		DBApp.createTable("l967h", cols1);
		for(int i=0;i<11;i++)
		{
			String [] record_l967h = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_l967h[j] = genRandString();
			}
			DBApp.insert("l967h", record_l967h);
		}
		int pagesCount_l967h = (int)Math.ceil(11.0/DBApp.dataPageSize);
		ArrayList<String> res_l967h = new ArrayList<>();
		for(int i=0;i<pagesCount_l967h;i++)
		{
			res_l967h.add(i+".db");
		}
		Collections.sort(res_l967h);
		String pagesTraceString_l967h = "l967h{ " + String.join(" ",res_l967h);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_l967h+")", fileManagerTrace1.contains(pagesTraceString_l967h));
		//--------------------------------------------------------------------------
		String[] cols2 = {"nqon","p505a","s46","f3t","jzu","nc6xv"};
		DBApp.createTable("a18bv", cols2);
		for(int i=0;i<75;i++)
		{
			String [] record_a18bv = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_a18bv[j] = genRandString();
			}
			DBApp.insert("a18bv", record_a18bv);
		}
		int pagesCount_a18bv = (int)Math.ceil(75.0/DBApp.dataPageSize);
		ArrayList<String> res_a18bv = new ArrayList<>();
		for(int i=0;i<pagesCount_a18bv;i++)
		{
			res_a18bv.add(i+".db");
		}
		Collections.sort(res_a18bv);
		String pagesTraceString_a18bv = "a18bv{ " + String.join(" ",res_a18bv);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_a18bv+")", fileManagerTrace2.contains(pagesTraceString_a18bv));
		//--------------------------------------------------------------------------
		String[] cols3 = {"vt"};
		DBApp.createTable("d04b", cols3);
		for(int i=0;i<32;i++)
		{
			String [] record_d04b = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_d04b[j] = genRandString();
			}
			DBApp.insert("d04b", record_d04b);
		}
		int pagesCount_d04b = (int)Math.ceil(32.0/DBApp.dataPageSize);
		ArrayList<String> res_d04b = new ArrayList<>();
		for(int i=0;i<pagesCount_d04b;i++)
		{
			res_d04b.add(i+".db");
		}
		Collections.sort(res_d04b);
		String pagesTraceString_d04b = "d04b{ " + String.join(" ",res_d04b);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_d04b+")", fileManagerTrace3.contains(pagesTraceString_d04b));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test25TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"y0","u8pby"};
		DBApp.createTable("ox4", cols0);
		for(int i=0;i<70;i++)
		{
			String [] record_ox4 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ox4[j] = genRandString();
			}
			DBApp.insert("ox4", record_ox4);
		}
		int pagesCount_ox4 = (int)Math.ceil(70.0/DBApp.dataPageSize);
		ArrayList<String> res_ox4 = new ArrayList<>();
		for(int i=0;i<pagesCount_ox4;i++)
		{
			res_ox4.add(i+".db");
		}
		Collections.sort(res_ox4);
		String pagesTraceString_ox4 = "ox4{ " + String.join(" ",res_ox4);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ox4+")", fileManagerTrace0.contains(pagesTraceString_ox4));
		//--------------------------------------------------------------------------
		String[] cols1 = {"rp09","ls6","oq768","p76","j4s","v66l","m0"};
		DBApp.createTable("pr2o2", cols1);
		for(int i=0;i<1;i++)
		{
			String [] record_pr2o2 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_pr2o2[j] = genRandString();
			}
			DBApp.insert("pr2o2", record_pr2o2);
		}
		int pagesCount_pr2o2 = (int)Math.ceil(1.0/DBApp.dataPageSize);
		ArrayList<String> res_pr2o2 = new ArrayList<>();
		for(int i=0;i<pagesCount_pr2o2;i++)
		{
			res_pr2o2.add(i+".db");
		}
		Collections.sort(res_pr2o2);
		String pagesTraceString_pr2o2 = "pr2o2{ " + String.join(" ",res_pr2o2);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_pr2o2+")", fileManagerTrace1.contains(pagesTraceString_pr2o2));
		//--------------------------------------------------------------------------
		String[] cols2 = {"y28","ef","b8oep","s5fo"};
		DBApp.createTable("x9", cols2);
		for(int i=0;i<12;i++)
		{
			String [] record_x9 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_x9[j] = genRandString();
			}
			DBApp.insert("x9", record_x9);
		}
		int pagesCount_x9 = (int)Math.ceil(12.0/DBApp.dataPageSize);
		ArrayList<String> res_x9 = new ArrayList<>();
		for(int i=0;i<pagesCount_x9;i++)
		{
			res_x9.add(i+".db");
		}
		Collections.sort(res_x9);
		String pagesTraceString_x9 = "x9{ " + String.join(" ",res_x9);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_x9+")", fileManagerTrace2.contains(pagesTraceString_x9));
		//--------------------------------------------------------------------------
		String[] cols3 = {"vk080"};
		DBApp.createTable("i4wc7", cols3);
		for(int i=0;i<38;i++)
		{
			String [] record_i4wc7 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_i4wc7[j] = genRandString();
			}
			DBApp.insert("i4wc7", record_i4wc7);
		}
		int pagesCount_i4wc7 = (int)Math.ceil(38.0/DBApp.dataPageSize);
		ArrayList<String> res_i4wc7 = new ArrayList<>();
		for(int i=0;i<pagesCount_i4wc7;i++)
		{
			res_i4wc7.add(i+".db");
		}
		Collections.sort(res_i4wc7);
		String pagesTraceString_i4wc7 = "i4wc7{ " + String.join(" ",res_i4wc7);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_i4wc7+")", fileManagerTrace3.contains(pagesTraceString_i4wc7));
		//--------------------------------------------------------------------------
		String[] cols4 = {"vya5","w2"};
		DBApp.createTable("m1", cols4);
		for(int i=0;i<94;i++)
		{
			String [] record_m1 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_m1[j] = genRandString();
			}
			DBApp.insert("m1", record_m1);
		}
		int pagesCount_m1 = (int)Math.ceil(94.0/DBApp.dataPageSize);
		ArrayList<String> res_m1 = new ArrayList<>();
		for(int i=0;i<pagesCount_m1;i++)
		{
			res_m1.add(i+".db");
		}
		Collections.sort(res_m1);
		String pagesTraceString_m1 = "m1{ " + String.join(" ",res_m1);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_m1+")", fileManagerTrace4.contains(pagesTraceString_m1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test26TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"zs70t","x92z","n7o","h89"};
		DBApp.createTable("o5", cols0);
		for(int i=0;i<11;i++)
		{
			String [] record_o5 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_o5[j] = genRandString();
			}
			DBApp.insert("o5", record_o5);
		}
		int pagesCount_o5 = (int)Math.ceil(11.0/DBApp.dataPageSize);
		ArrayList<String> res_o5 = new ArrayList<>();
		for(int i=0;i<pagesCount_o5;i++)
		{
			res_o5.add(i+".db");
		}
		Collections.sort(res_o5);
		String pagesTraceString_o5 = "o5{ " + String.join(" ",res_o5);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_o5+")", fileManagerTrace0.contains(pagesTraceString_o5));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ev02"};
		DBApp.createTable("d3m", cols1);
		for(int i=0;i<96;i++)
		{
			String [] record_d3m = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_d3m[j] = genRandString();
			}
			DBApp.insert("d3m", record_d3m);
		}
		int pagesCount_d3m = (int)Math.ceil(96.0/DBApp.dataPageSize);
		ArrayList<String> res_d3m = new ArrayList<>();
		for(int i=0;i<pagesCount_d3m;i++)
		{
			res_d3m.add(i+".db");
		}
		Collections.sort(res_d3m);
		String pagesTraceString_d3m = "d3m{ " + String.join(" ",res_d3m);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_d3m+")", fileManagerTrace1.contains(pagesTraceString_d3m));
		//--------------------------------------------------------------------------
		String[] cols2 = {"l2","k7t6j","f9","tn9","c52","kwd","d86"};
		DBApp.createTable("fzp40", cols2);
		for(int i=0;i<34;i++)
		{
			String [] record_fzp40 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_fzp40[j] = genRandString();
			}
			DBApp.insert("fzp40", record_fzp40);
		}
		int pagesCount_fzp40 = (int)Math.ceil(34.0/DBApp.dataPageSize);
		ArrayList<String> res_fzp40 = new ArrayList<>();
		for(int i=0;i<pagesCount_fzp40;i++)
		{
			res_fzp40.add(i+".db");
		}
		Collections.sort(res_fzp40);
		String pagesTraceString_fzp40 = "fzp40{ " + String.join(" ",res_fzp40);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_fzp40+")", fileManagerTrace2.contains(pagesTraceString_fzp40));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test27TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"d3b","ho49q","h21gl","l5a62","z1pg4","q15"};
		DBApp.createTable("f41", cols0);
		for(int i=0;i<26;i++)
		{
			String [] record_f41 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_f41[j] = genRandString();
			}
			DBApp.insert("f41", record_f41);
		}
		int pagesCount_f41 = (int)Math.ceil(26.0/DBApp.dataPageSize);
		ArrayList<String> res_f41 = new ArrayList<>();
		for(int i=0;i<pagesCount_f41;i++)
		{
			res_f41.add(i+".db");
		}
		Collections.sort(res_f41);
		String pagesTraceString_f41 = "f41{ " + String.join(" ",res_f41);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_f41+")", fileManagerTrace0.contains(pagesTraceString_f41));
		//--------------------------------------------------------------------------
		String[] cols1 = {"vn8","lmmz4"};
		DBApp.createTable("w9", cols1);
		for(int i=0;i<14;i++)
		{
			String [] record_w9 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_w9[j] = genRandString();
			}
			DBApp.insert("w9", record_w9);
		}
		int pagesCount_w9 = (int)Math.ceil(14.0/DBApp.dataPageSize);
		ArrayList<String> res_w9 = new ArrayList<>();
		for(int i=0;i<pagesCount_w9;i++)
		{
			res_w9.add(i+".db");
		}
		Collections.sort(res_w9);
		String pagesTraceString_w9 = "w9{ " + String.join(" ",res_w9);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_w9+")", fileManagerTrace1.contains(pagesTraceString_w9));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test28TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"sr96","m49","h9c","u9"};
		DBApp.createTable("z1w", cols0);
		for(int i=0;i<90;i++)
		{
			String [] record_z1w = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_z1w[j] = genRandString();
			}
			DBApp.insert("z1w", record_z1w);
		}
		int pagesCount_z1w = (int)Math.ceil(90.0/DBApp.dataPageSize);
		ArrayList<String> res_z1w = new ArrayList<>();
		for(int i=0;i<pagesCount_z1w;i++)
		{
			res_z1w.add(i+".db");
		}
		Collections.sort(res_z1w);
		String pagesTraceString_z1w = "z1w{ " + String.join(" ",res_z1w);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_z1w+")", fileManagerTrace0.contains(pagesTraceString_z1w));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ab1","altz","mrz","jopig","t2rmm","e9","p72","o50wb","b24"};
		DBApp.createTable("zdh", cols1);
		for(int i=0;i<70;i++)
		{
			String [] record_zdh = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_zdh[j] = genRandString();
			}
			DBApp.insert("zdh", record_zdh);
		}
		int pagesCount_zdh = (int)Math.ceil(70.0/DBApp.dataPageSize);
		ArrayList<String> res_zdh = new ArrayList<>();
		for(int i=0;i<pagesCount_zdh;i++)
		{
			res_zdh.add(i+".db");
		}
		Collections.sort(res_zdh);
		String pagesTraceString_zdh = "zdh{ " + String.join(" ",res_zdh);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_zdh+")", fileManagerTrace1.contains(pagesTraceString_zdh));
		//--------------------------------------------------------------------------
		String[] cols2 = {"w04"};
		DBApp.createTable("xp0w", cols2);
		for(int i=0;i<48;i++)
		{
			String [] record_xp0w = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_xp0w[j] = genRandString();
			}
			DBApp.insert("xp0w", record_xp0w);
		}
		int pagesCount_xp0w = (int)Math.ceil(48.0/DBApp.dataPageSize);
		ArrayList<String> res_xp0w = new ArrayList<>();
		for(int i=0;i<pagesCount_xp0w;i++)
		{
			res_xp0w.add(i+".db");
		}
		Collections.sort(res_xp0w);
		String pagesTraceString_xp0w = "xp0w{ " + String.join(" ",res_xp0w);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_xp0w+")", fileManagerTrace2.contains(pagesTraceString_xp0w));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test29TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"oh","sc0z","s0","mu0","mp8","s5","hu4","o875","g69"};
		DBApp.createTable("r6o", cols0);
		for(int i=0;i<92;i++)
		{
			String [] record_r6o = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_r6o[j] = genRandString();
			}
			DBApp.insert("r6o", record_r6o);
		}
		int pagesCount_r6o = (int)Math.ceil(92.0/DBApp.dataPageSize);
		ArrayList<String> res_r6o = new ArrayList<>();
		for(int i=0;i<pagesCount_r6o;i++)
		{
			res_r6o.add(i+".db");
		}
		Collections.sort(res_r6o);
		String pagesTraceString_r6o = "r6o{ " + String.join(" ",res_r6o);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_r6o+")", fileManagerTrace0.contains(pagesTraceString_r6o));
		//--------------------------------------------------------------------------
		String[] cols1 = {"x340","aid","x8gn","k8","h3240","vt5","zhj2a"};
		DBApp.createTable("c49", cols1);
		for(int i=0;i<25;i++)
		{
			String [] record_c49 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_c49[j] = genRandString();
			}
			DBApp.insert("c49", record_c49);
		}
		int pagesCount_c49 = (int)Math.ceil(25.0/DBApp.dataPageSize);
		ArrayList<String> res_c49 = new ArrayList<>();
		for(int i=0;i<pagesCount_c49;i++)
		{
			res_c49.add(i+".db");
		}
		Collections.sort(res_c49);
		String pagesTraceString_c49 = "c49{ " + String.join(" ",res_c49);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_c49+")", fileManagerTrace1.contains(pagesTraceString_c49));
		//--------------------------------------------------------------------------
		String[] cols2 = {"h5","hw","j2f3d","dxs8","z2g8y"};
		DBApp.createTable("idq6d", cols2);
		for(int i=0;i<95;i++)
		{
			String [] record_idq6d = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_idq6d[j] = genRandString();
			}
			DBApp.insert("idq6d", record_idq6d);
		}
		int pagesCount_idq6d = (int)Math.ceil(95.0/DBApp.dataPageSize);
		ArrayList<String> res_idq6d = new ArrayList<>();
		for(int i=0;i<pagesCount_idq6d;i++)
		{
			res_idq6d.add(i+".db");
		}
		Collections.sort(res_idq6d);
		String pagesTraceString_idq6d = "idq6d{ " + String.join(" ",res_idq6d);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_idq6d+")", fileManagerTrace2.contains(pagesTraceString_idq6d));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a4","yzx","h4","v9w","vt","gcu0n","d9f","gm","xfr3"};
		DBApp.createTable("radt4", cols3);
		for(int i=0;i<43;i++)
		{
			String [] record_radt4 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_radt4[j] = genRandString();
			}
			DBApp.insert("radt4", record_radt4);
		}
		int pagesCount_radt4 = (int)Math.ceil(43.0/DBApp.dataPageSize);
		ArrayList<String> res_radt4 = new ArrayList<>();
		for(int i=0;i<pagesCount_radt4;i++)
		{
			res_radt4.add(i+".db");
		}
		Collections.sort(res_radt4);
		String pagesTraceString_radt4 = "radt4{ " + String.join(" ",res_radt4);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_radt4+")", fileManagerTrace3.contains(pagesTraceString_radt4));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test30TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"i6627","d40o","j2zj","i4","l34uw","e31w","s1x68","al","ev"};
		DBApp.createTable("xyb5", cols0);
		for(int i=0;i<67;i++)
		{
			String [] record_xyb5 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_xyb5[j] = genRandString();
			}
			DBApp.insert("xyb5", record_xyb5);
		}
		int pagesCount_xyb5 = (int)Math.ceil(67.0/DBApp.dataPageSize);
		ArrayList<String> res_xyb5 = new ArrayList<>();
		for(int i=0;i<pagesCount_xyb5;i++)
		{
			res_xyb5.add(i+".db");
		}
		Collections.sort(res_xyb5);
		String pagesTraceString_xyb5 = "xyb5{ " + String.join(" ",res_xyb5);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_xyb5+")", fileManagerTrace0.contains(pagesTraceString_xyb5));
		//--------------------------------------------------------------------------
		String[] cols1 = {"f4fr","vt75","d09","a7004","x1u3f","c3os","oaxy","q6g"};
		DBApp.createTable("fq4n", cols1);
		for(int i=0;i<7;i++)
		{
			String [] record_fq4n = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_fq4n[j] = genRandString();
			}
			DBApp.insert("fq4n", record_fq4n);
		}
		int pagesCount_fq4n = (int)Math.ceil(7.0/DBApp.dataPageSize);
		ArrayList<String> res_fq4n = new ArrayList<>();
		for(int i=0;i<pagesCount_fq4n;i++)
		{
			res_fq4n.add(i+".db");
		}
		Collections.sort(res_fq4n);
		String pagesTraceString_fq4n = "fq4n{ " + String.join(" ",res_fq4n);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_fq4n+")", fileManagerTrace1.contains(pagesTraceString_fq4n));
		//--------------------------------------------------------------------------
		String[] cols2 = {"ujz13","rax64","l4j","vkr","h58"};
		DBApp.createTable("j7", cols2);
		for(int i=0;i<53;i++)
		{
			String [] record_j7 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_j7[j] = genRandString();
			}
			DBApp.insert("j7", record_j7);
		}
		int pagesCount_j7 = (int)Math.ceil(53.0/DBApp.dataPageSize);
		ArrayList<String> res_j7 = new ArrayList<>();
		for(int i=0;i<pagesCount_j7;i++)
		{
			res_j7.add(i+".db");
		}
		Collections.sort(res_j7);
		String pagesTraceString_j7 = "j7{ " + String.join(" ",res_j7);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_j7+")", fileManagerTrace2.contains(pagesTraceString_j7));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test31TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"f907","v3","bq16","q77","h7","kyi","s2","w9at6","uxca"};
		DBApp.createTable("v52", cols0);
		for(int i=0;i<64;i++)
		{
			String [] record_v52 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_v52[j] = genRandString();
			}
			DBApp.insert("v52", record_v52);
		}
		int pagesCount_v52 = (int)Math.ceil(64.0/DBApp.dataPageSize);
		ArrayList<String> res_v52 = new ArrayList<>();
		for(int i=0;i<pagesCount_v52;i++)
		{
			res_v52.add(i+".db");
		}
		Collections.sort(res_v52);
		String pagesTraceString_v52 = "v52{ " + String.join(" ",res_v52);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_v52+")", fileManagerTrace0.contains(pagesTraceString_v52));
		//--------------------------------------------------------------------------
		String[] cols1 = {"n7lxa","gnp8","s17","q5y","k6yr1","a75","gl6","l4k4","usr"};
		DBApp.createTable("z41", cols1);
		for(int i=0;i<20;i++)
		{
			String [] record_z41 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_z41[j] = genRandString();
			}
			DBApp.insert("z41", record_z41);
		}
		int pagesCount_z41 = (int)Math.ceil(20.0/DBApp.dataPageSize);
		ArrayList<String> res_z41 = new ArrayList<>();
		for(int i=0;i<pagesCount_z41;i++)
		{
			res_z41.add(i+".db");
		}
		Collections.sort(res_z41);
		String pagesTraceString_z41 = "z41{ " + String.join(" ",res_z41);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_z41+")", fileManagerTrace1.contains(pagesTraceString_z41));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test32TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"tnd","t07","xn99","dm","s6u","hnz7w"};
		DBApp.createTable("f4478", cols0);
		for(int i=0;i<76;i++)
		{
			String [] record_f4478 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_f4478[j] = genRandString();
			}
			DBApp.insert("f4478", record_f4478);
		}
		int pagesCount_f4478 = (int)Math.ceil(76.0/DBApp.dataPageSize);
		ArrayList<String> res_f4478 = new ArrayList<>();
		for(int i=0;i<pagesCount_f4478;i++)
		{
			res_f4478.add(i+".db");
		}
		Collections.sort(res_f4478);
		String pagesTraceString_f4478 = "f4478{ " + String.join(" ",res_f4478);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_f4478+")", fileManagerTrace0.contains(pagesTraceString_f4478));
		//--------------------------------------------------------------------------
		String[] cols1 = {"l3q","p3y","tse","cerh"};
		DBApp.createTable("r6", cols1);
		for(int i=0;i<47;i++)
		{
			String [] record_r6 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_r6[j] = genRandString();
			}
			DBApp.insert("r6", record_r6);
		}
		int pagesCount_r6 = (int)Math.ceil(47.0/DBApp.dataPageSize);
		ArrayList<String> res_r6 = new ArrayList<>();
		for(int i=0;i<pagesCount_r6;i++)
		{
			res_r6.add(i+".db");
		}
		Collections.sort(res_r6);
		String pagesTraceString_r6 = "r6{ " + String.join(" ",res_r6);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_r6+")", fileManagerTrace1.contains(pagesTraceString_r6));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test33TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"dz3","bq1","ar","p177","s5","z86","k8y","pp3"};
		DBApp.createTable("ede", cols0);
		for(int i=0;i<4;i++)
		{
			String [] record_ede = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ede[j] = genRandString();
			}
			DBApp.insert("ede", record_ede);
		}
		int pagesCount_ede = (int)Math.ceil(4.0/DBApp.dataPageSize);
		ArrayList<String> res_ede = new ArrayList<>();
		for(int i=0;i<pagesCount_ede;i++)
		{
			res_ede.add(i+".db");
		}
		Collections.sort(res_ede);
		String pagesTraceString_ede = "ede{ " + String.join(" ",res_ede);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ede+")", fileManagerTrace0.contains(pagesTraceString_ede));
		//--------------------------------------------------------------------------
		String[] cols1 = {"q5v","sw3my"};
		DBApp.createTable("a44", cols1);
		for(int i=0;i<67;i++)
		{
			String [] record_a44 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_a44[j] = genRandString();
			}
			DBApp.insert("a44", record_a44);
		}
		int pagesCount_a44 = (int)Math.ceil(67.0/DBApp.dataPageSize);
		ArrayList<String> res_a44 = new ArrayList<>();
		for(int i=0;i<pagesCount_a44;i++)
		{
			res_a44.add(i+".db");
		}
		Collections.sort(res_a44);
		String pagesTraceString_a44 = "a44{ " + String.join(" ",res_a44);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_a44+")", fileManagerTrace1.contains(pagesTraceString_a44));
		//--------------------------------------------------------------------------
		String[] cols2 = {"x3o5","nk","n0z","b4","wv2","bbjyb","bd0","oqjj","s97","ox5"};
		DBApp.createTable("pb5j", cols2);
		for(int i=0;i<57;i++)
		{
			String [] record_pb5j = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_pb5j[j] = genRandString();
			}
			DBApp.insert("pb5j", record_pb5j);
		}
		int pagesCount_pb5j = (int)Math.ceil(57.0/DBApp.dataPageSize);
		ArrayList<String> res_pb5j = new ArrayList<>();
		for(int i=0;i<pagesCount_pb5j;i++)
		{
			res_pb5j.add(i+".db");
		}
		Collections.sort(res_pb5j);
		String pagesTraceString_pb5j = "pb5j{ " + String.join(" ",res_pb5j);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_pb5j+")", fileManagerTrace2.contains(pagesTraceString_pb5j));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test34TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"c2"};
		DBApp.createTable("k4y", cols0);
		for(int i=0;i<11;i++)
		{
			String [] record_k4y = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_k4y[j] = genRandString();
			}
			DBApp.insert("k4y", record_k4y);
		}
		int pagesCount_k4y = (int)Math.ceil(11.0/DBApp.dataPageSize);
		ArrayList<String> res_k4y = new ArrayList<>();
		for(int i=0;i<pagesCount_k4y;i++)
		{
			res_k4y.add(i+".db");
		}
		Collections.sort(res_k4y);
		String pagesTraceString_k4y = "k4y{ " + String.join(" ",res_k4y);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_k4y+")", fileManagerTrace0.contains(pagesTraceString_k4y));
		//--------------------------------------------------------------------------
		String[] cols1 = {"r08f7","f2a","np15n"};
		DBApp.createTable("i283", cols1);
		for(int i=0;i<52;i++)
		{
			String [] record_i283 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_i283[j] = genRandString();
			}
			DBApp.insert("i283", record_i283);
		}
		int pagesCount_i283 = (int)Math.ceil(52.0/DBApp.dataPageSize);
		ArrayList<String> res_i283 = new ArrayList<>();
		for(int i=0;i<pagesCount_i283;i++)
		{
			res_i283.add(i+".db");
		}
		Collections.sort(res_i283);
		String pagesTraceString_i283 = "i283{ " + String.join(" ",res_i283);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_i283+")", fileManagerTrace1.contains(pagesTraceString_i283));
		//--------------------------------------------------------------------------
		String[] cols2 = {"ij","igt","r84","x0z1w","tcf","ae75","jk","il92f"};
		DBApp.createTable("z0b", cols2);
		for(int i=0;i<77;i++)
		{
			String [] record_z0b = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_z0b[j] = genRandString();
			}
			DBApp.insert("z0b", record_z0b);
		}
		int pagesCount_z0b = (int)Math.ceil(77.0/DBApp.dataPageSize);
		ArrayList<String> res_z0b = new ArrayList<>();
		for(int i=0;i<pagesCount_z0b;i++)
		{
			res_z0b.add(i+".db");
		}
		Collections.sort(res_z0b);
		String pagesTraceString_z0b = "z0b{ " + String.join(" ",res_z0b);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_z0b+")", fileManagerTrace2.contains(pagesTraceString_z0b));
		//--------------------------------------------------------------------------
		String[] cols3 = {"wh9","u2b"};
		DBApp.createTable("qgq", cols3);
		for(int i=0;i<55;i++)
		{
			String [] record_qgq = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_qgq[j] = genRandString();
			}
			DBApp.insert("qgq", record_qgq);
		}
		int pagesCount_qgq = (int)Math.ceil(55.0/DBApp.dataPageSize);
		ArrayList<String> res_qgq = new ArrayList<>();
		for(int i=0;i<pagesCount_qgq;i++)
		{
			res_qgq.add(i+".db");
		}
		Collections.sort(res_qgq);
		String pagesTraceString_qgq = "qgq{ " + String.join(" ",res_qgq);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_qgq+")", fileManagerTrace3.contains(pagesTraceString_qgq));
		//--------------------------------------------------------------------------
		String[] cols4 = {"w0o96","cd00z","o85","lep","hw","giam","i9v","e82u","vd6","wh5n"};
		DBApp.createTable("w77", cols4);
		for(int i=0;i<56;i++)
		{
			String [] record_w77 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_w77[j] = genRandString();
			}
			DBApp.insert("w77", record_w77);
		}
		int pagesCount_w77 = (int)Math.ceil(56.0/DBApp.dataPageSize);
		ArrayList<String> res_w77 = new ArrayList<>();
		for(int i=0;i<pagesCount_w77;i++)
		{
			res_w77.add(i+".db");
		}
		Collections.sort(res_w77);
		String pagesTraceString_w77 = "w77{ " + String.join(" ",res_w77);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_w77+")", fileManagerTrace4.contains(pagesTraceString_w77));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test35TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"n4","cc5z9","s8ti","x0h","u40","p60","bh18","ry7","yqm"};
		DBApp.createTable("vg4", cols0);
		for(int i=0;i<100;i++)
		{
			String [] record_vg4 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_vg4[j] = genRandString();
			}
			DBApp.insert("vg4", record_vg4);
		}
		int pagesCount_vg4 = (int)Math.ceil(100.0/DBApp.dataPageSize);
		ArrayList<String> res_vg4 = new ArrayList<>();
		for(int i=0;i<pagesCount_vg4;i++)
		{
			res_vg4.add(i+".db");
		}
		Collections.sort(res_vg4);
		String pagesTraceString_vg4 = "vg4{ " + String.join(" ",res_vg4);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_vg4+")", fileManagerTrace0.contains(pagesTraceString_vg4));
		//--------------------------------------------------------------------------
		String[] cols1 = {"fp0k6","fpgi","b9u","u0d","i94","i59"};
		DBApp.createTable("mu", cols1);
		for(int i=0;i<12;i++)
		{
			String [] record_mu = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_mu[j] = genRandString();
			}
			DBApp.insert("mu", record_mu);
		}
		int pagesCount_mu = (int)Math.ceil(12.0/DBApp.dataPageSize);
		ArrayList<String> res_mu = new ArrayList<>();
		for(int i=0;i<pagesCount_mu;i++)
		{
			res_mu.add(i+".db");
		}
		Collections.sort(res_mu);
		String pagesTraceString_mu = "mu{ " + String.join(" ",res_mu);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_mu+")", fileManagerTrace1.contains(pagesTraceString_mu));
		//--------------------------------------------------------------------------
		String[] cols2 = {"xn","d0e","lq","km"};
		DBApp.createTable("j9r", cols2);
		for(int i=0;i<36;i++)
		{
			String [] record_j9r = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_j9r[j] = genRandString();
			}
			DBApp.insert("j9r", record_j9r);
		}
		int pagesCount_j9r = (int)Math.ceil(36.0/DBApp.dataPageSize);
		ArrayList<String> res_j9r = new ArrayList<>();
		for(int i=0;i<pagesCount_j9r;i++)
		{
			res_j9r.add(i+".db");
		}
		Collections.sort(res_j9r);
		String pagesTraceString_j9r = "j9r{ " + String.join(" ",res_j9r);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_j9r+")", fileManagerTrace2.contains(pagesTraceString_j9r));
		//--------------------------------------------------------------------------
		String[] cols3 = {"dj5c2","i5","o2","q4","x79","kutq6","vh","ra9","z9","m0z"};
		DBApp.createTable("m2g5", cols3);
		for(int i=0;i<26;i++)
		{
			String [] record_m2g5 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_m2g5[j] = genRandString();
			}
			DBApp.insert("m2g5", record_m2g5);
		}
		int pagesCount_m2g5 = (int)Math.ceil(26.0/DBApp.dataPageSize);
		ArrayList<String> res_m2g5 = new ArrayList<>();
		for(int i=0;i<pagesCount_m2g5;i++)
		{
			res_m2g5.add(i+".db");
		}
		Collections.sort(res_m2g5);
		String pagesTraceString_m2g5 = "m2g5{ " + String.join(" ",res_m2g5);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_m2g5+")", fileManagerTrace3.contains(pagesTraceString_m2g5));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test36TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"t3q","l9r","m1","h41ah","ta6","bry","qa707"};
		DBApp.createTable("c69", cols0);
		for(int i=0;i<40;i++)
		{
			String [] record_c69 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_c69[j] = genRandString();
			}
			DBApp.insert("c69", record_c69);
		}
		int pagesCount_c69 = (int)Math.ceil(40.0/DBApp.dataPageSize);
		ArrayList<String> res_c69 = new ArrayList<>();
		for(int i=0;i<pagesCount_c69;i++)
		{
			res_c69.add(i+".db");
		}
		Collections.sort(res_c69);
		String pagesTraceString_c69 = "c69{ " + String.join(" ",res_c69);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_c69+")", fileManagerTrace0.contains(pagesTraceString_c69));
		//--------------------------------------------------------------------------
		String[] cols1 = {"g9m","k151s","n65fg","j947c"};
		DBApp.createTable("zx6", cols1);
		for(int i=0;i<85;i++)
		{
			String [] record_zx6 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_zx6[j] = genRandString();
			}
			DBApp.insert("zx6", record_zx6);
		}
		int pagesCount_zx6 = (int)Math.ceil(85.0/DBApp.dataPageSize);
		ArrayList<String> res_zx6 = new ArrayList<>();
		for(int i=0;i<pagesCount_zx6;i++)
		{
			res_zx6.add(i+".db");
		}
		Collections.sort(res_zx6);
		String pagesTraceString_zx6 = "zx6{ " + String.join(" ",res_zx6);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_zx6+")", fileManagerTrace1.contains(pagesTraceString_zx6));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test37TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"kb5","t7m","x03","ykf","g03io"};
		DBApp.createTable("u57o", cols0);
		for(int i=0;i<65;i++)
		{
			String [] record_u57o = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_u57o[j] = genRandString();
			}
			DBApp.insert("u57o", record_u57o);
		}
		int pagesCount_u57o = (int)Math.ceil(65.0/DBApp.dataPageSize);
		ArrayList<String> res_u57o = new ArrayList<>();
		for(int i=0;i<pagesCount_u57o;i++)
		{
			res_u57o.add(i+".db");
		}
		Collections.sort(res_u57o);
		String pagesTraceString_u57o = "u57o{ " + String.join(" ",res_u57o);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_u57o+")", fileManagerTrace0.contains(pagesTraceString_u57o));
		//--------------------------------------------------------------------------
		String[] cols1 = {"l72","k98"};
		DBApp.createTable("z80", cols1);
		for(int i=0;i<93;i++)
		{
			String [] record_z80 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_z80[j] = genRandString();
			}
			DBApp.insert("z80", record_z80);
		}
		int pagesCount_z80 = (int)Math.ceil(93.0/DBApp.dataPageSize);
		ArrayList<String> res_z80 = new ArrayList<>();
		for(int i=0;i<pagesCount_z80;i++)
		{
			res_z80.add(i+".db");
		}
		Collections.sort(res_z80);
		String pagesTraceString_z80 = "z80{ " + String.join(" ",res_z80);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_z80+")", fileManagerTrace1.contains(pagesTraceString_z80));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test38TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"v9d","y5","e32cn","g2y","c1c","ozp","gh7","r5"};
		DBApp.createTable("n8075", cols0);
		for(int i=0;i<28;i++)
		{
			String [] record_n8075 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_n8075[j] = genRandString();
			}
			DBApp.insert("n8075", record_n8075);
		}
		int pagesCount_n8075 = (int)Math.ceil(28.0/DBApp.dataPageSize);
		ArrayList<String> res_n8075 = new ArrayList<>();
		for(int i=0;i<pagesCount_n8075;i++)
		{
			res_n8075.add(i+".db");
		}
		Collections.sort(res_n8075);
		String pagesTraceString_n8075 = "n8075{ " + String.join(" ",res_n8075);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_n8075+")", fileManagerTrace0.contains(pagesTraceString_n8075));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ph","k38","m3b9","j904","lpc2a","m2t","j7904","g6"};
		DBApp.createTable("uve0", cols1);
		for(int i=0;i<62;i++)
		{
			String [] record_uve0 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_uve0[j] = genRandString();
			}
			DBApp.insert("uve0", record_uve0);
		}
		int pagesCount_uve0 = (int)Math.ceil(62.0/DBApp.dataPageSize);
		ArrayList<String> res_uve0 = new ArrayList<>();
		for(int i=0;i<pagesCount_uve0;i++)
		{
			res_uve0.add(i+".db");
		}
		Collections.sort(res_uve0);
		String pagesTraceString_uve0 = "uve0{ " + String.join(" ",res_uve0);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_uve0+")", fileManagerTrace1.contains(pagesTraceString_uve0));
		//--------------------------------------------------------------------------
		String[] cols2 = {"zb1","vm9r","cy4v7"};
		DBApp.createTable("urfe", cols2);
		for(int i=0;i<37;i++)
		{
			String [] record_urfe = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_urfe[j] = genRandString();
			}
			DBApp.insert("urfe", record_urfe);
		}
		int pagesCount_urfe = (int)Math.ceil(37.0/DBApp.dataPageSize);
		ArrayList<String> res_urfe = new ArrayList<>();
		for(int i=0;i<pagesCount_urfe;i++)
		{
			res_urfe.add(i+".db");
		}
		Collections.sort(res_urfe);
		String pagesTraceString_urfe = "urfe{ " + String.join(" ",res_urfe);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_urfe+")", fileManagerTrace2.contains(pagesTraceString_urfe));
		//--------------------------------------------------------------------------
		String[] cols3 = {"c047","qz6g","leu48","p3mzt","o9","ez682","rc5h"};
		DBApp.createTable("j3k", cols3);
		for(int i=0;i<46;i++)
		{
			String [] record_j3k = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_j3k[j] = genRandString();
			}
			DBApp.insert("j3k", record_j3k);
		}
		int pagesCount_j3k = (int)Math.ceil(46.0/DBApp.dataPageSize);
		ArrayList<String> res_j3k = new ArrayList<>();
		for(int i=0;i<pagesCount_j3k;i++)
		{
			res_j3k.add(i+".db");
		}
		Collections.sort(res_j3k);
		String pagesTraceString_j3k = "j3k{ " + String.join(" ",res_j3k);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_j3k+")", fileManagerTrace3.contains(pagesTraceString_j3k));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test39TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"k21","ny","gtu8","f20","rpe","i6"};
		DBApp.createTable("v52p4", cols0);
		for(int i=0;i<45;i++)
		{
			String [] record_v52p4 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_v52p4[j] = genRandString();
			}
			DBApp.insert("v52p4", record_v52p4);
		}
		int pagesCount_v52p4 = (int)Math.ceil(45.0/DBApp.dataPageSize);
		ArrayList<String> res_v52p4 = new ArrayList<>();
		for(int i=0;i<pagesCount_v52p4;i++)
		{
			res_v52p4.add(i+".db");
		}
		Collections.sort(res_v52p4);
		String pagesTraceString_v52p4 = "v52p4{ " + String.join(" ",res_v52p4);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_v52p4+")", fileManagerTrace0.contains(pagesTraceString_v52p4));
		//--------------------------------------------------------------------------
		String[] cols1 = {"zew2w"};
		DBApp.createTable("adt", cols1);
		for(int i=0;i<59;i++)
		{
			String [] record_adt = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_adt[j] = genRandString();
			}
			DBApp.insert("adt", record_adt);
		}
		int pagesCount_adt = (int)Math.ceil(59.0/DBApp.dataPageSize);
		ArrayList<String> res_adt = new ArrayList<>();
		for(int i=0;i<pagesCount_adt;i++)
		{
			res_adt.add(i+".db");
		}
		Collections.sort(res_adt);
		String pagesTraceString_adt = "adt{ " + String.join(" ",res_adt);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_adt+")", fileManagerTrace1.contains(pagesTraceString_adt));
		//--------------------------------------------------------------------------
		String[] cols2 = {"m59ca","ad6"};
		DBApp.createTable("ib", cols2);
		for(int i=0;i<38;i++)
		{
			String [] record_ib = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_ib[j] = genRandString();
			}
			DBApp.insert("ib", record_ib);
		}
		int pagesCount_ib = (int)Math.ceil(38.0/DBApp.dataPageSize);
		ArrayList<String> res_ib = new ArrayList<>();
		for(int i=0;i<pagesCount_ib;i++)
		{
			res_ib.add(i+".db");
		}
		Collections.sort(res_ib);
		String pagesTraceString_ib = "ib{ " + String.join(" ",res_ib);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ib+")", fileManagerTrace2.contains(pagesTraceString_ib));
		//--------------------------------------------------------------------------
		String[] cols3 = {"fx","tz2"};
		DBApp.createTable("ggq", cols3);
		for(int i=0;i<20;i++)
		{
			String [] record_ggq = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_ggq[j] = genRandString();
			}
			DBApp.insert("ggq", record_ggq);
		}
		int pagesCount_ggq = (int)Math.ceil(20.0/DBApp.dataPageSize);
		ArrayList<String> res_ggq = new ArrayList<>();
		for(int i=0;i<pagesCount_ggq;i++)
		{
			res_ggq.add(i+".db");
		}
		Collections.sort(res_ggq);
		String pagesTraceString_ggq = "ggq{ " + String.join(" ",res_ggq);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ggq+")", fileManagerTrace3.contains(pagesTraceString_ggq));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test40TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"p807","hxn","da4v","vc","a6w4m","yf105"};
		DBApp.createTable("ggqbm", cols0);
		for(int i=0;i<56;i++)
		{
			String [] record_ggqbm = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ggqbm[j] = genRandString();
			}
			DBApp.insert("ggqbm", record_ggqbm);
		}
		int pagesCount_ggqbm = (int)Math.ceil(56.0/DBApp.dataPageSize);
		ArrayList<String> res_ggqbm = new ArrayList<>();
		for(int i=0;i<pagesCount_ggqbm;i++)
		{
			res_ggqbm.add(i+".db");
		}
		Collections.sort(res_ggqbm);
		String pagesTraceString_ggqbm = "ggqbm{ " + String.join(" ",res_ggqbm);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ggqbm+")", fileManagerTrace0.contains(pagesTraceString_ggqbm));
		//--------------------------------------------------------------------------
		String[] cols1 = {"eu6d","b0","vk","bknos","fb6","pyz","e4","a58s"};
		DBApp.createTable("b81b0", cols1);
		for(int i=0;i<15;i++)
		{
			String [] record_b81b0 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_b81b0[j] = genRandString();
			}
			DBApp.insert("b81b0", record_b81b0);
		}
		int pagesCount_b81b0 = (int)Math.ceil(15.0/DBApp.dataPageSize);
		ArrayList<String> res_b81b0 = new ArrayList<>();
		for(int i=0;i<pagesCount_b81b0;i++)
		{
			res_b81b0.add(i+".db");
		}
		Collections.sort(res_b81b0);
		String pagesTraceString_b81b0 = "b81b0{ " + String.join(" ",res_b81b0);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_b81b0+")", fileManagerTrace1.contains(pagesTraceString_b81b0));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test41TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"bc","bu","d3","t0o","vs573","wtc00","d0y","fhe"};
		DBApp.createTable("v3li", cols0);
		for(int i=0;i<24;i++)
		{
			String [] record_v3li = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_v3li[j] = genRandString();
			}
			DBApp.insert("v3li", record_v3li);
		}
		int pagesCount_v3li = (int)Math.ceil(24.0/DBApp.dataPageSize);
		ArrayList<String> res_v3li = new ArrayList<>();
		for(int i=0;i<pagesCount_v3li;i++)
		{
			res_v3li.add(i+".db");
		}
		Collections.sort(res_v3li);
		String pagesTraceString_v3li = "v3li{ " + String.join(" ",res_v3li);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_v3li+")", fileManagerTrace0.contains(pagesTraceString_v3li));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test42TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"xm2","t6309","t5roe"};
		DBApp.createTable("a79", cols0);
		for(int i=0;i<59;i++)
		{
			String [] record_a79 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_a79[j] = genRandString();
			}
			DBApp.insert("a79", record_a79);
		}
		int pagesCount_a79 = (int)Math.ceil(59.0/DBApp.dataPageSize);
		ArrayList<String> res_a79 = new ArrayList<>();
		for(int i=0;i<pagesCount_a79;i++)
		{
			res_a79.add(i+".db");
		}
		Collections.sort(res_a79);
		String pagesTraceString_a79 = "a79{ " + String.join(" ",res_a79);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_a79+")", fileManagerTrace0.contains(pagesTraceString_a79));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test43TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"v0b"};
		DBApp.createTable("g3", cols0);
		for(int i=0;i<7;i++)
		{
			String [] record_g3 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_g3[j] = genRandString();
			}
			DBApp.insert("g3", record_g3);
		}
		int pagesCount_g3 = (int)Math.ceil(7.0/DBApp.dataPageSize);
		ArrayList<String> res_g3 = new ArrayList<>();
		for(int i=0;i<pagesCount_g3;i++)
		{
			res_g3.add(i+".db");
		}
		Collections.sort(res_g3);
		String pagesTraceString_g3 = "g3{ " + String.join(" ",res_g3);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_g3+")", fileManagerTrace0.contains(pagesTraceString_g3));
		//--------------------------------------------------------------------------
		String[] cols1 = {"f31eo","jho","pox","z0","vu1l"};
		DBApp.createTable("mf", cols1);
		for(int i=0;i<36;i++)
		{
			String [] record_mf = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_mf[j] = genRandString();
			}
			DBApp.insert("mf", record_mf);
		}
		int pagesCount_mf = (int)Math.ceil(36.0/DBApp.dataPageSize);
		ArrayList<String> res_mf = new ArrayList<>();
		for(int i=0;i<pagesCount_mf;i++)
		{
			res_mf.add(i+".db");
		}
		Collections.sort(res_mf);
		String pagesTraceString_mf = "mf{ " + String.join(" ",res_mf);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_mf+")", fileManagerTrace1.contains(pagesTraceString_mf));
		//--------------------------------------------------------------------------
		String[] cols2 = {"i52","t95zj"};
		DBApp.createTable("m04", cols2);
		for(int i=0;i<24;i++)
		{
			String [] record_m04 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_m04[j] = genRandString();
			}
			DBApp.insert("m04", record_m04);
		}
		int pagesCount_m04 = (int)Math.ceil(24.0/DBApp.dataPageSize);
		ArrayList<String> res_m04 = new ArrayList<>();
		for(int i=0;i<pagesCount_m04;i++)
		{
			res_m04.add(i+".db");
		}
		Collections.sort(res_m04);
		String pagesTraceString_m04 = "m04{ " + String.join(" ",res_m04);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_m04+")", fileManagerTrace2.contains(pagesTraceString_m04));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test44TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"hd98","h0","rug","n449","ex8","k2f","b24l"};
		DBApp.createTable("xlz4", cols0);
		for(int i=0;i<53;i++)
		{
			String [] record_xlz4 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_xlz4[j] = genRandString();
			}
			DBApp.insert("xlz4", record_xlz4);
		}
		int pagesCount_xlz4 = (int)Math.ceil(53.0/DBApp.dataPageSize);
		ArrayList<String> res_xlz4 = new ArrayList<>();
		for(int i=0;i<pagesCount_xlz4;i++)
		{
			res_xlz4.add(i+".db");
		}
		Collections.sort(res_xlz4);
		String pagesTraceString_xlz4 = "xlz4{ " + String.join(" ",res_xlz4);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_xlz4+")", fileManagerTrace0.contains(pagesTraceString_xlz4));
		//--------------------------------------------------------------------------
		String[] cols1 = {"wc0","aq","fup8e"};
		DBApp.createTable("h8", cols1);
		for(int i=0;i<52;i++)
		{
			String [] record_h8 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_h8[j] = genRandString();
			}
			DBApp.insert("h8", record_h8);
		}
		int pagesCount_h8 = (int)Math.ceil(52.0/DBApp.dataPageSize);
		ArrayList<String> res_h8 = new ArrayList<>();
		for(int i=0;i<pagesCount_h8;i++)
		{
			res_h8.add(i+".db");
		}
		Collections.sort(res_h8);
		String pagesTraceString_h8 = "h8{ " + String.join(" ",res_h8);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_h8+")", fileManagerTrace1.contains(pagesTraceString_h8));
		//--------------------------------------------------------------------------
		String[] cols2 = {"nq3","u02n","lt670","n0z","os6","xfb"};
		DBApp.createTable("e13", cols2);
		for(int i=0;i<75;i++)
		{
			String [] record_e13 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_e13[j] = genRandString();
			}
			DBApp.insert("e13", record_e13);
		}
		int pagesCount_e13 = (int)Math.ceil(75.0/DBApp.dataPageSize);
		ArrayList<String> res_e13 = new ArrayList<>();
		for(int i=0;i<pagesCount_e13;i++)
		{
			res_e13.add(i+".db");
		}
		Collections.sort(res_e13);
		String pagesTraceString_e13 = "e13{ " + String.join(" ",res_e13);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_e13+")", fileManagerTrace2.contains(pagesTraceString_e13));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test45TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"n9s","z30zd","h2ei","n1g","hc","ix2"};
		DBApp.createTable("la51f", cols0);
		for(int i=0;i<56;i++)
		{
			String [] record_la51f = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_la51f[j] = genRandString();
			}
			DBApp.insert("la51f", record_la51f);
		}
		int pagesCount_la51f = (int)Math.ceil(56.0/DBApp.dataPageSize);
		ArrayList<String> res_la51f = new ArrayList<>();
		for(int i=0;i<pagesCount_la51f;i++)
		{
			res_la51f.add(i+".db");
		}
		Collections.sort(res_la51f);
		String pagesTraceString_la51f = "la51f{ " + String.join(" ",res_la51f);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_la51f+")", fileManagerTrace0.contains(pagesTraceString_la51f));
		//--------------------------------------------------------------------------
		String[] cols1 = {"z47"};
		DBApp.createTable("uq0", cols1);
		for(int i=0;i<30;i++)
		{
			String [] record_uq0 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_uq0[j] = genRandString();
			}
			DBApp.insert("uq0", record_uq0);
		}
		int pagesCount_uq0 = (int)Math.ceil(30.0/DBApp.dataPageSize);
		ArrayList<String> res_uq0 = new ArrayList<>();
		for(int i=0;i<pagesCount_uq0;i++)
		{
			res_uq0.add(i+".db");
		}
		Collections.sort(res_uq0);
		String pagesTraceString_uq0 = "uq0{ " + String.join(" ",res_uq0);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_uq0+")", fileManagerTrace1.contains(pagesTraceString_uq0));
		//--------------------------------------------------------------------------
		String[] cols2 = {"h5o7","v033","hll1e","k37","cro","y95qt","w8r","ayh","h7","ejf"};
		DBApp.createTable("fq", cols2);
		for(int i=0;i<36;i++)
		{
			String [] record_fq = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_fq[j] = genRandString();
			}
			DBApp.insert("fq", record_fq);
		}
		int pagesCount_fq = (int)Math.ceil(36.0/DBApp.dataPageSize);
		ArrayList<String> res_fq = new ArrayList<>();
		for(int i=0;i<pagesCount_fq;i++)
		{
			res_fq.add(i+".db");
		}
		Collections.sort(res_fq);
		String pagesTraceString_fq = "fq{ " + String.join(" ",res_fq);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_fq+")", fileManagerTrace2.contains(pagesTraceString_fq));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test46TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"nr00","o3","zt2","y4vq","tx7","r8wv"};
		DBApp.createTable("mtk", cols0);
		for(int i=0;i<98;i++)
		{
			String [] record_mtk = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_mtk[j] = genRandString();
			}
			DBApp.insert("mtk", record_mtk);
		}
		int pagesCount_mtk = (int)Math.ceil(98.0/DBApp.dataPageSize);
		ArrayList<String> res_mtk = new ArrayList<>();
		for(int i=0;i<pagesCount_mtk;i++)
		{
			res_mtk.add(i+".db");
		}
		Collections.sort(res_mtk);
		String pagesTraceString_mtk = "mtk{ " + String.join(" ",res_mtk);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_mtk+")", fileManagerTrace0.contains(pagesTraceString_mtk));
		//--------------------------------------------------------------------------
		String[] cols1 = {"mc5","h96","u74t","g7","dw","ngef"};
		DBApp.createTable("fo74i", cols1);
		for(int i=0;i<31;i++)
		{
			String [] record_fo74i = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_fo74i[j] = genRandString();
			}
			DBApp.insert("fo74i", record_fo74i);
		}
		int pagesCount_fo74i = (int)Math.ceil(31.0/DBApp.dataPageSize);
		ArrayList<String> res_fo74i = new ArrayList<>();
		for(int i=0;i<pagesCount_fo74i;i++)
		{
			res_fo74i.add(i+".db");
		}
		Collections.sort(res_fo74i);
		String pagesTraceString_fo74i = "fo74i{ " + String.join(" ",res_fo74i);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_fo74i+")", fileManagerTrace1.contains(pagesTraceString_fo74i));
		//--------------------------------------------------------------------------
		String[] cols2 = {"c24","tp1h","h8l","fg4b","ip","v6","mz6","k66","uub8","l83av"};
		DBApp.createTable("m401", cols2);
		for(int i=0;i<40;i++)
		{
			String [] record_m401 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_m401[j] = genRandString();
			}
			DBApp.insert("m401", record_m401);
		}
		int pagesCount_m401 = (int)Math.ceil(40.0/DBApp.dataPageSize);
		ArrayList<String> res_m401 = new ArrayList<>();
		for(int i=0;i<pagesCount_m401;i++)
		{
			res_m401.add(i+".db");
		}
		Collections.sort(res_m401);
		String pagesTraceString_m401 = "m401{ " + String.join(" ",res_m401);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_m401+")", fileManagerTrace2.contains(pagesTraceString_m401));
		//--------------------------------------------------------------------------
		String[] cols3 = {"hx4","kqa8","am63","c77"};
		DBApp.createTable("u7g", cols3);
		for(int i=0;i<67;i++)
		{
			String [] record_u7g = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_u7g[j] = genRandString();
			}
			DBApp.insert("u7g", record_u7g);
		}
		int pagesCount_u7g = (int)Math.ceil(67.0/DBApp.dataPageSize);
		ArrayList<String> res_u7g = new ArrayList<>();
		for(int i=0;i<pagesCount_u7g;i++)
		{
			res_u7g.add(i+".db");
		}
		Collections.sort(res_u7g);
		String pagesTraceString_u7g = "u7g{ " + String.join(" ",res_u7g);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_u7g+")", fileManagerTrace3.contains(pagesTraceString_u7g));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test47TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"hk6j4","f7sk","fl"};
		DBApp.createTable("wx", cols0);
		for(int i=0;i<36;i++)
		{
			String [] record_wx = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_wx[j] = genRandString();
			}
			DBApp.insert("wx", record_wx);
		}
		int pagesCount_wx = (int)Math.ceil(36.0/DBApp.dataPageSize);
		ArrayList<String> res_wx = new ArrayList<>();
		for(int i=0;i<pagesCount_wx;i++)
		{
			res_wx.add(i+".db");
		}
		Collections.sort(res_wx);
		String pagesTraceString_wx = "wx{ " + String.join(" ",res_wx);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_wx+")", fileManagerTrace0.contains(pagesTraceString_wx));
		//--------------------------------------------------------------------------
		String[] cols1 = {"irck","s2"};
		DBApp.createTable("knr", cols1);
		for(int i=0;i<67;i++)
		{
			String [] record_knr = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_knr[j] = genRandString();
			}
			DBApp.insert("knr", record_knr);
		}
		int pagesCount_knr = (int)Math.ceil(67.0/DBApp.dataPageSize);
		ArrayList<String> res_knr = new ArrayList<>();
		for(int i=0;i<pagesCount_knr;i++)
		{
			res_knr.add(i+".db");
		}
		Collections.sort(res_knr);
		String pagesTraceString_knr = "knr{ " + String.join(" ",res_knr);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_knr+")", fileManagerTrace1.contains(pagesTraceString_knr));
		//--------------------------------------------------------------------------
		String[] cols2 = {"fh5","j612j","q1","vi0","m81","a018","ul608","i2","hcwmn"};
		DBApp.createTable("r9lsg", cols2);
		for(int i=0;i<14;i++)
		{
			String [] record_r9lsg = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_r9lsg[j] = genRandString();
			}
			DBApp.insert("r9lsg", record_r9lsg);
		}
		int pagesCount_r9lsg = (int)Math.ceil(14.0/DBApp.dataPageSize);
		ArrayList<String> res_r9lsg = new ArrayList<>();
		for(int i=0;i<pagesCount_r9lsg;i++)
		{
			res_r9lsg.add(i+".db");
		}
		Collections.sort(res_r9lsg);
		String pagesTraceString_r9lsg = "r9lsg{ " + String.join(" ",res_r9lsg);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_r9lsg+")", fileManagerTrace2.contains(pagesTraceString_r9lsg));
		//--------------------------------------------------------------------------
		String[] cols3 = {"t6","yihu"};
		DBApp.createTable("maor8", cols3);
		for(int i=0;i<27;i++)
		{
			String [] record_maor8 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_maor8[j] = genRandString();
			}
			DBApp.insert("maor8", record_maor8);
		}
		int pagesCount_maor8 = (int)Math.ceil(27.0/DBApp.dataPageSize);
		ArrayList<String> res_maor8 = new ArrayList<>();
		for(int i=0;i<pagesCount_maor8;i++)
		{
			res_maor8.add(i+".db");
		}
		Collections.sort(res_maor8);
		String pagesTraceString_maor8 = "maor8{ " + String.join(" ",res_maor8);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_maor8+")", fileManagerTrace3.contains(pagesTraceString_maor8));
		//--------------------------------------------------------------------------
		String[] cols4 = {"lk","u5","f5","y5","dhkap","ilmo6","kgg","k05","t10"};
		DBApp.createTable("r1z", cols4);
		for(int i=0;i<95;i++)
		{
			String [] record_r1z = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_r1z[j] = genRandString();
			}
			DBApp.insert("r1z", record_r1z);
		}
		int pagesCount_r1z = (int)Math.ceil(95.0/DBApp.dataPageSize);
		ArrayList<String> res_r1z = new ArrayList<>();
		for(int i=0;i<pagesCount_r1z;i++)
		{
			res_r1z.add(i+".db");
		}
		Collections.sort(res_r1z);
		String pagesTraceString_r1z = "r1z{ " + String.join(" ",res_r1z);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_r1z+")", fileManagerTrace4.contains(pagesTraceString_r1z));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test48TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"zq20x","ayu5t","s14m1","ku6y","yr3"};
		DBApp.createTable("g51", cols0);
		for(int i=0;i<22;i++)
		{
			String [] record_g51 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_g51[j] = genRandString();
			}
			DBApp.insert("g51", record_g51);
		}
		int pagesCount_g51 = (int)Math.ceil(22.0/DBApp.dataPageSize);
		ArrayList<String> res_g51 = new ArrayList<>();
		for(int i=0;i<pagesCount_g51;i++)
		{
			res_g51.add(i+".db");
		}
		Collections.sort(res_g51);
		String pagesTraceString_g51 = "g51{ " + String.join(" ",res_g51);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_g51+")", fileManagerTrace0.contains(pagesTraceString_g51));
		//--------------------------------------------------------------------------
		String[] cols1 = {"zw4tb","szr1","l0i","kco","w15p"};
		DBApp.createTable("us3o", cols1);
		for(int i=0;i<66;i++)
		{
			String [] record_us3o = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_us3o[j] = genRandString();
			}
			DBApp.insert("us3o", record_us3o);
		}
		int pagesCount_us3o = (int)Math.ceil(66.0/DBApp.dataPageSize);
		ArrayList<String> res_us3o = new ArrayList<>();
		for(int i=0;i<pagesCount_us3o;i++)
		{
			res_us3o.add(i+".db");
		}
		Collections.sort(res_us3o);
		String pagesTraceString_us3o = "us3o{ " + String.join(" ",res_us3o);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_us3o+")", fileManagerTrace1.contains(pagesTraceString_us3o));
		//--------------------------------------------------------------------------
		String[] cols2 = {"qrf3"};
		DBApp.createTable("vk3", cols2);
		for(int i=0;i<30;i++)
		{
			String [] record_vk3 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_vk3[j] = genRandString();
			}
			DBApp.insert("vk3", record_vk3);
		}
		int pagesCount_vk3 = (int)Math.ceil(30.0/DBApp.dataPageSize);
		ArrayList<String> res_vk3 = new ArrayList<>();
		for(int i=0;i<pagesCount_vk3;i++)
		{
			res_vk3.add(i+".db");
		}
		Collections.sort(res_vk3);
		String pagesTraceString_vk3 = "vk3{ " + String.join(" ",res_vk3);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_vk3+")", fileManagerTrace2.contains(pagesTraceString_vk3));
		//--------------------------------------------------------------------------
		String[] cols3 = {"d9","rg7","hx","g31t","ao9"};
		DBApp.createTable("qs534", cols3);
		for(int i=0;i<30;i++)
		{
			String [] record_qs534 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_qs534[j] = genRandString();
			}
			DBApp.insert("qs534", record_qs534);
		}
		int pagesCount_qs534 = (int)Math.ceil(30.0/DBApp.dataPageSize);
		ArrayList<String> res_qs534 = new ArrayList<>();
		for(int i=0;i<pagesCount_qs534;i++)
		{
			res_qs534.add(i+".db");
		}
		Collections.sort(res_qs534);
		String pagesTraceString_qs534 = "qs534{ " + String.join(" ",res_qs534);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_qs534+")", fileManagerTrace3.contains(pagesTraceString_qs534));
		//--------------------------------------------------------------------------
		String[] cols4 = {"z5","x8p","bds0c"};
		DBApp.createTable("woy6", cols4);
		for(int i=0;i<93;i++)
		{
			String [] record_woy6 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_woy6[j] = genRandString();
			}
			DBApp.insert("woy6", record_woy6);
		}
		int pagesCount_woy6 = (int)Math.ceil(93.0/DBApp.dataPageSize);
		ArrayList<String> res_woy6 = new ArrayList<>();
		for(int i=0;i<pagesCount_woy6;i++)
		{
			res_woy6.add(i+".db");
		}
		Collections.sort(res_woy6);
		String pagesTraceString_woy6 = "woy6{ " + String.join(" ",res_woy6);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_woy6+")", fileManagerTrace4.contains(pagesTraceString_woy6));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test49TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"s7zs","iql"};
		DBApp.createTable("cy", cols0);
		for(int i=0;i<30;i++)
		{
			String [] record_cy = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_cy[j] = genRandString();
			}
			DBApp.insert("cy", record_cy);
		}
		int pagesCount_cy = (int)Math.ceil(30.0/DBApp.dataPageSize);
		ArrayList<String> res_cy = new ArrayList<>();
		for(int i=0;i<pagesCount_cy;i++)
		{
			res_cy.add(i+".db");
		}
		Collections.sort(res_cy);
		String pagesTraceString_cy = "cy{ " + String.join(" ",res_cy);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_cy+")", fileManagerTrace0.contains(pagesTraceString_cy));
		//--------------------------------------------------------------------------
		String[] cols1 = {"s6","po","a9"};
		DBApp.createTable("q9c0", cols1);
		for(int i=0;i<87;i++)
		{
			String [] record_q9c0 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_q9c0[j] = genRandString();
			}
			DBApp.insert("q9c0", record_q9c0);
		}
		int pagesCount_q9c0 = (int)Math.ceil(87.0/DBApp.dataPageSize);
		ArrayList<String> res_q9c0 = new ArrayList<>();
		for(int i=0;i<pagesCount_q9c0;i++)
		{
			res_q9c0.add(i+".db");
		}
		Collections.sort(res_q9c0);
		String pagesTraceString_q9c0 = "q9c0{ " + String.join(" ",res_q9c0);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_q9c0+")", fileManagerTrace1.contains(pagesTraceString_q9c0));
		//--------------------------------------------------------------------------
		String[] cols2 = {"pw6u3","qx6","qdn","k97","wdcm"};
		DBApp.createTable("h55", cols2);
		for(int i=0;i<75;i++)
		{
			String [] record_h55 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_h55[j] = genRandString();
			}
			DBApp.insert("h55", record_h55);
		}
		int pagesCount_h55 = (int)Math.ceil(75.0/DBApp.dataPageSize);
		ArrayList<String> res_h55 = new ArrayList<>();
		for(int i=0;i<pagesCount_h55;i++)
		{
			res_h55.add(i+".db");
		}
		Collections.sort(res_h55);
		String pagesTraceString_h55 = "h55{ " + String.join(" ",res_h55);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_h55+")", fileManagerTrace2.contains(pagesTraceString_h55));
		//--------------------------------------------------------------------------
		String[] cols3 = {"tue","lc55","rc5","u6dtj","fw","a1f","ci5","k2ek"};
		DBApp.createTable("w58e5", cols3);
		for(int i=0;i<3;i++)
		{
			String [] record_w58e5 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_w58e5[j] = genRandString();
			}
			DBApp.insert("w58e5", record_w58e5);
		}
		int pagesCount_w58e5 = (int)Math.ceil(3.0/DBApp.dataPageSize);
		ArrayList<String> res_w58e5 = new ArrayList<>();
		for(int i=0;i<pagesCount_w58e5;i++)
		{
			res_w58e5.add(i+".db");
		}
		Collections.sort(res_w58e5);
		String pagesTraceString_w58e5 = "w58e5{ " + String.join(" ",res_w58e5);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_w58e5+")", fileManagerTrace3.contains(pagesTraceString_w58e5));
		//--------------------------------------------------------------------------
		String[] cols4 = {"k9g","o9mpd","dwv","nu0k","mxb","l3","p1","i5876","m98r"};
		DBApp.createTable("zlc", cols4);
		for(int i=0;i<31;i++)
		{
			String [] record_zlc = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_zlc[j] = genRandString();
			}
			DBApp.insert("zlc", record_zlc);
		}
		int pagesCount_zlc = (int)Math.ceil(31.0/DBApp.dataPageSize);
		ArrayList<String> res_zlc = new ArrayList<>();
		for(int i=0;i<pagesCount_zlc;i++)
		{
			res_zlc.add(i+".db");
		}
		Collections.sort(res_zlc);
		String pagesTraceString_zlc = "zlc{ " + String.join(" ",res_zlc);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_zlc+")", fileManagerTrace4.contains(pagesTraceString_zlc));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test50TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"ywgd","ul7m0","u0z","esx","wy81","t0","ct335","m2928","ha8"};
		DBApp.createTable("ra", cols0);
		for(int i=0;i<85;i++)
		{
			String [] record_ra = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ra[j] = genRandString();
			}
			DBApp.insert("ra", record_ra);
		}
		int pagesCount_ra = (int)Math.ceil(85.0/DBApp.dataPageSize);
		ArrayList<String> res_ra = new ArrayList<>();
		for(int i=0;i<pagesCount_ra;i++)
		{
			res_ra.add(i+".db");
		}
		Collections.sort(res_ra);
		String pagesTraceString_ra = "ra{ " + String.join(" ",res_ra);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ra+")", fileManagerTrace0.contains(pagesTraceString_ra));
		//--------------------------------------------------------------------------
		String[] cols1 = {"d4","o8a","drcp4","pgx88","a5"};
		DBApp.createTable("ps", cols1);
		for(int i=0;i<50;i++)
		{
			String [] record_ps = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_ps[j] = genRandString();
			}
			DBApp.insert("ps", record_ps);
		}
		int pagesCount_ps = (int)Math.ceil(50.0/DBApp.dataPageSize);
		ArrayList<String> res_ps = new ArrayList<>();
		for(int i=0;i<pagesCount_ps;i++)
		{
			res_ps.add(i+".db");
		}
		Collections.sort(res_ps);
		String pagesTraceString_ps = "ps{ " + String.join(" ",res_ps);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ps+")", fileManagerTrace1.contains(pagesTraceString_ps));
		//--------------------------------------------------------------------------
		String[] cols2 = {"ch19","f6wc","m8vy1"};
		DBApp.createTable("u0r7", cols2);
		for(int i=0;i<16;i++)
		{
			String [] record_u0r7 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_u0r7[j] = genRandString();
			}
			DBApp.insert("u0r7", record_u0r7);
		}
		int pagesCount_u0r7 = (int)Math.ceil(16.0/DBApp.dataPageSize);
		ArrayList<String> res_u0r7 = new ArrayList<>();
		for(int i=0;i<pagesCount_u0r7;i++)
		{
			res_u0r7.add(i+".db");
		}
		Collections.sort(res_u0r7);
		String pagesTraceString_u0r7 = "u0r7{ " + String.join(" ",res_u0r7);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_u0r7+")", fileManagerTrace2.contains(pagesTraceString_u0r7));
		//--------------------------------------------------------------------------
		String[] cols3 = {"av5","a7u","v4"};
		DBApp.createTable("j435", cols3);
		for(int i=0;i<90;i++)
		{
			String [] record_j435 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_j435[j] = genRandString();
			}
			DBApp.insert("j435", record_j435);
		}
		int pagesCount_j435 = (int)Math.ceil(90.0/DBApp.dataPageSize);
		ArrayList<String> res_j435 = new ArrayList<>();
		for(int i=0;i<pagesCount_j435;i++)
		{
			res_j435.add(i+".db");
		}
		Collections.sort(res_j435);
		String pagesTraceString_j435 = "j435{ " + String.join(" ",res_j435);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_j435+")", fileManagerTrace3.contains(pagesTraceString_j435));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test51TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"oq4","ya9","a73","q63","x5a1s","r04w4","s66","nz7","xe4","juwh"};
		DBApp.createTable("aghl", cols0);
		for(int i=0;i<57;i++)
		{
			String [] record_aghl = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_aghl[j] = genRandString();
			}
			DBApp.insert("aghl", record_aghl);
		}
		int pagesCount_aghl = (int)Math.ceil(57.0/DBApp.dataPageSize);
		ArrayList<String> res_aghl = new ArrayList<>();
		for(int i=0;i<pagesCount_aghl;i++)
		{
			res_aghl.add(i+".db");
		}
		Collections.sort(res_aghl);
		String pagesTraceString_aghl = "aghl{ " + String.join(" ",res_aghl);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_aghl+")", fileManagerTrace0.contains(pagesTraceString_aghl));
		//--------------------------------------------------------------------------
		String[] cols1 = {"pgh","nz1","bq1","wex84"};
		DBApp.createTable("z5a", cols1);
		for(int i=0;i<72;i++)
		{
			String [] record_z5a = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_z5a[j] = genRandString();
			}
			DBApp.insert("z5a", record_z5a);
		}
		int pagesCount_z5a = (int)Math.ceil(72.0/DBApp.dataPageSize);
		ArrayList<String> res_z5a = new ArrayList<>();
		for(int i=0;i<pagesCount_z5a;i++)
		{
			res_z5a.add(i+".db");
		}
		Collections.sort(res_z5a);
		String pagesTraceString_z5a = "z5a{ " + String.join(" ",res_z5a);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_z5a+")", fileManagerTrace1.contains(pagesTraceString_z5a));
		//--------------------------------------------------------------------------
		String[] cols2 = {"h7s","tm3","p92","m76","s61x","o3"};
		DBApp.createTable("ywf", cols2);
		for(int i=0;i<62;i++)
		{
			String [] record_ywf = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_ywf[j] = genRandString();
			}
			DBApp.insert("ywf", record_ywf);
		}
		int pagesCount_ywf = (int)Math.ceil(62.0/DBApp.dataPageSize);
		ArrayList<String> res_ywf = new ArrayList<>();
		for(int i=0;i<pagesCount_ywf;i++)
		{
			res_ywf.add(i+".db");
		}
		Collections.sort(res_ywf);
		String pagesTraceString_ywf = "ywf{ " + String.join(" ",res_ywf);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ywf+")", fileManagerTrace2.contains(pagesTraceString_ywf));
		//--------------------------------------------------------------------------
		String[] cols3 = {"i2m"};
		DBApp.createTable("n11", cols3);
		for(int i=0;i<93;i++)
		{
			String [] record_n11 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_n11[j] = genRandString();
			}
			DBApp.insert("n11", record_n11);
		}
		int pagesCount_n11 = (int)Math.ceil(93.0/DBApp.dataPageSize);
		ArrayList<String> res_n11 = new ArrayList<>();
		for(int i=0;i<pagesCount_n11;i++)
		{
			res_n11.add(i+".db");
		}
		Collections.sort(res_n11);
		String pagesTraceString_n11 = "n11{ " + String.join(" ",res_n11);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_n11+")", fileManagerTrace3.contains(pagesTraceString_n11));
		//--------------------------------------------------------------------------
		String[] cols4 = {"ub9j"};
		DBApp.createTable("p6n", cols4);
		for(int i=0;i<31;i++)
		{
			String [] record_p6n = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_p6n[j] = genRandString();
			}
			DBApp.insert("p6n", record_p6n);
		}
		int pagesCount_p6n = (int)Math.ceil(31.0/DBApp.dataPageSize);
		ArrayList<String> res_p6n = new ArrayList<>();
		for(int i=0;i<pagesCount_p6n;i++)
		{
			res_p6n.add(i+".db");
		}
		Collections.sort(res_p6n);
		String pagesTraceString_p6n = "p6n{ " + String.join(" ",res_p6n);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_p6n+")", fileManagerTrace4.contains(pagesTraceString_p6n));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test52TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"m39","a7c","hznm"};
		DBApp.createTable("atr4", cols0);
		for(int i=0;i<16;i++)
		{
			String [] record_atr4 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_atr4[j] = genRandString();
			}
			DBApp.insert("atr4", record_atr4);
		}
		int pagesCount_atr4 = (int)Math.ceil(16.0/DBApp.dataPageSize);
		ArrayList<String> res_atr4 = new ArrayList<>();
		for(int i=0;i<pagesCount_atr4;i++)
		{
			res_atr4.add(i+".db");
		}
		Collections.sort(res_atr4);
		String pagesTraceString_atr4 = "atr4{ " + String.join(" ",res_atr4);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_atr4+")", fileManagerTrace0.contains(pagesTraceString_atr4));
		//--------------------------------------------------------------------------
		String[] cols1 = {"kyu","iko","t2k9l","cmfu","kk6","b1ks1","s2","c75oj","c77","gs5"};
		DBApp.createTable("ot", cols1);
		for(int i=0;i<100;i++)
		{
			String [] record_ot = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_ot[j] = genRandString();
			}
			DBApp.insert("ot", record_ot);
		}
		int pagesCount_ot = (int)Math.ceil(100.0/DBApp.dataPageSize);
		ArrayList<String> res_ot = new ArrayList<>();
		for(int i=0;i<pagesCount_ot;i++)
		{
			res_ot.add(i+".db");
		}
		Collections.sort(res_ot);
		String pagesTraceString_ot = "ot{ " + String.join(" ",res_ot);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ot+")", fileManagerTrace1.contains(pagesTraceString_ot));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test53TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"q04"};
		DBApp.createTable("nr77r", cols0);
		for(int i=0;i<98;i++)
		{
			String [] record_nr77r = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_nr77r[j] = genRandString();
			}
			DBApp.insert("nr77r", record_nr77r);
		}
		int pagesCount_nr77r = (int)Math.ceil(98.0/DBApp.dataPageSize);
		ArrayList<String> res_nr77r = new ArrayList<>();
		for(int i=0;i<pagesCount_nr77r;i++)
		{
			res_nr77r.add(i+".db");
		}
		Collections.sort(res_nr77r);
		String pagesTraceString_nr77r = "nr77r{ " + String.join(" ",res_nr77r);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_nr77r+")", fileManagerTrace0.contains(pagesTraceString_nr77r));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ngw","sc8","guyx","k9w","m8","se8"};
		DBApp.createTable("sg3", cols1);
		for(int i=0;i<75;i++)
		{
			String [] record_sg3 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_sg3[j] = genRandString();
			}
			DBApp.insert("sg3", record_sg3);
		}
		int pagesCount_sg3 = (int)Math.ceil(75.0/DBApp.dataPageSize);
		ArrayList<String> res_sg3 = new ArrayList<>();
		for(int i=0;i<pagesCount_sg3;i++)
		{
			res_sg3.add(i+".db");
		}
		Collections.sort(res_sg3);
		String pagesTraceString_sg3 = "sg3{ " + String.join(" ",res_sg3);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_sg3+")", fileManagerTrace1.contains(pagesTraceString_sg3));
		//--------------------------------------------------------------------------
		String[] cols2 = {"ce89","s1877","co2","d44h3","fj9r","zjy7v","i08pd","gfi","onf","nr"};
		DBApp.createTable("ng3n", cols2);
		for(int i=0;i<71;i++)
		{
			String [] record_ng3n = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_ng3n[j] = genRandString();
			}
			DBApp.insert("ng3n", record_ng3n);
		}
		int pagesCount_ng3n = (int)Math.ceil(71.0/DBApp.dataPageSize);
		ArrayList<String> res_ng3n = new ArrayList<>();
		for(int i=0;i<pagesCount_ng3n;i++)
		{
			res_ng3n.add(i+".db");
		}
		Collections.sort(res_ng3n);
		String pagesTraceString_ng3n = "ng3n{ " + String.join(" ",res_ng3n);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ng3n+")", fileManagerTrace2.contains(pagesTraceString_ng3n));
		//--------------------------------------------------------------------------
		String[] cols3 = {"d6x","a737"};
		DBApp.createTable("j4t42", cols3);
		for(int i=0;i<46;i++)
		{
			String [] record_j4t42 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_j4t42[j] = genRandString();
			}
			DBApp.insert("j4t42", record_j4t42);
		}
		int pagesCount_j4t42 = (int)Math.ceil(46.0/DBApp.dataPageSize);
		ArrayList<String> res_j4t42 = new ArrayList<>();
		for(int i=0;i<pagesCount_j4t42;i++)
		{
			res_j4t42.add(i+".db");
		}
		Collections.sort(res_j4t42);
		String pagesTraceString_j4t42 = "j4t42{ " + String.join(" ",res_j4t42);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_j4t42+")", fileManagerTrace3.contains(pagesTraceString_j4t42));
		//--------------------------------------------------------------------------
		String[] cols4 = {"u3","mi","j1j","a6j2","df","rv97d","y3c","m96k","il8o","k4"};
		DBApp.createTable("zn2", cols4);
		for(int i=0;i<77;i++)
		{
			String [] record_zn2 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_zn2[j] = genRandString();
			}
			DBApp.insert("zn2", record_zn2);
		}
		int pagesCount_zn2 = (int)Math.ceil(77.0/DBApp.dataPageSize);
		ArrayList<String> res_zn2 = new ArrayList<>();
		for(int i=0;i<pagesCount_zn2;i++)
		{
			res_zn2.add(i+".db");
		}
		Collections.sort(res_zn2);
		String pagesTraceString_zn2 = "zn2{ " + String.join(" ",res_zn2);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_zn2+")", fileManagerTrace4.contains(pagesTraceString_zn2));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test54TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"gm5","i6g","z6","r45fq","iz1o3","ec7","ppyi","owp"};
		DBApp.createTable("h9", cols0);
		for(int i=0;i<98;i++)
		{
			String [] record_h9 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_h9[j] = genRandString();
			}
			DBApp.insert("h9", record_h9);
		}
		int pagesCount_h9 = (int)Math.ceil(98.0/DBApp.dataPageSize);
		ArrayList<String> res_h9 = new ArrayList<>();
		for(int i=0;i<pagesCount_h9;i++)
		{
			res_h9.add(i+".db");
		}
		Collections.sort(res_h9);
		String pagesTraceString_h9 = "h9{ " + String.join(" ",res_h9);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_h9+")", fileManagerTrace0.contains(pagesTraceString_h9));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test55TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"d6u5","n61u8"};
		DBApp.createTable("ela", cols0);
		for(int i=0;i<93;i++)
		{
			String [] record_ela = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ela[j] = genRandString();
			}
			DBApp.insert("ela", record_ela);
		}
		int pagesCount_ela = (int)Math.ceil(93.0/DBApp.dataPageSize);
		ArrayList<String> res_ela = new ArrayList<>();
		for(int i=0;i<pagesCount_ela;i++)
		{
			res_ela.add(i+".db");
		}
		Collections.sort(res_ela);
		String pagesTraceString_ela = "ela{ " + String.join(" ",res_ela);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ela+")", fileManagerTrace0.contains(pagesTraceString_ela));
		//--------------------------------------------------------------------------
		String[] cols1 = {"yll","vpe","k24","z2o","dm0","g3u","xj","o9m","at1"};
		DBApp.createTable("i2", cols1);
		for(int i=0;i<93;i++)
		{
			String [] record_i2 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_i2[j] = genRandString();
			}
			DBApp.insert("i2", record_i2);
		}
		int pagesCount_i2 = (int)Math.ceil(93.0/DBApp.dataPageSize);
		ArrayList<String> res_i2 = new ArrayList<>();
		for(int i=0;i<pagesCount_i2;i++)
		{
			res_i2.add(i+".db");
		}
		Collections.sort(res_i2);
		String pagesTraceString_i2 = "i2{ " + String.join(" ",res_i2);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_i2+")", fileManagerTrace1.contains(pagesTraceString_i2));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test56TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"d8","o1","j4","dj","w0a","a8sr","wp13f"};
		DBApp.createTable("j4", cols0);
		for(int i=0;i<1;i++)
		{
			String [] record_j4 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_j4[j] = genRandString();
			}
			DBApp.insert("j4", record_j4);
		}
		int pagesCount_j4 = (int)Math.ceil(1.0/DBApp.dataPageSize);
		ArrayList<String> res_j4 = new ArrayList<>();
		for(int i=0;i<pagesCount_j4;i++)
		{
			res_j4.add(i+".db");
		}
		Collections.sort(res_j4);
		String pagesTraceString_j4 = "j4{ " + String.join(" ",res_j4);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_j4+")", fileManagerTrace0.contains(pagesTraceString_j4));
		//--------------------------------------------------------------------------
		String[] cols1 = {"jr","j11"};
		DBApp.createTable("u70", cols1);
		for(int i=0;i<42;i++)
		{
			String [] record_u70 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_u70[j] = genRandString();
			}
			DBApp.insert("u70", record_u70);
		}
		int pagesCount_u70 = (int)Math.ceil(42.0/DBApp.dataPageSize);
		ArrayList<String> res_u70 = new ArrayList<>();
		for(int i=0;i<pagesCount_u70;i++)
		{
			res_u70.add(i+".db");
		}
		Collections.sort(res_u70);
		String pagesTraceString_u70 = "u70{ " + String.join(" ",res_u70);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_u70+")", fileManagerTrace1.contains(pagesTraceString_u70));
		//--------------------------------------------------------------------------
		String[] cols2 = {"ie301","qt7","x60","q3i02"};
		DBApp.createTable("h0", cols2);
		for(int i=0;i<35;i++)
		{
			String [] record_h0 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_h0[j] = genRandString();
			}
			DBApp.insert("h0", record_h0);
		}
		int pagesCount_h0 = (int)Math.ceil(35.0/DBApp.dataPageSize);
		ArrayList<String> res_h0 = new ArrayList<>();
		for(int i=0;i<pagesCount_h0;i++)
		{
			res_h0.add(i+".db");
		}
		Collections.sort(res_h0);
		String pagesTraceString_h0 = "h0{ " + String.join(" ",res_h0);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_h0+")", fileManagerTrace2.contains(pagesTraceString_h0));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test57TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"f0a","ej","g1wd","ct","o7h5"};
		DBApp.createTable("dds", cols0);
		for(int i=0;i<39;i++)
		{
			String [] record_dds = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_dds[j] = genRandString();
			}
			DBApp.insert("dds", record_dds);
		}
		int pagesCount_dds = (int)Math.ceil(39.0/DBApp.dataPageSize);
		ArrayList<String> res_dds = new ArrayList<>();
		for(int i=0;i<pagesCount_dds;i++)
		{
			res_dds.add(i+".db");
		}
		Collections.sort(res_dds);
		String pagesTraceString_dds = "dds{ " + String.join(" ",res_dds);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_dds+")", fileManagerTrace0.contains(pagesTraceString_dds));
		//--------------------------------------------------------------------------
		String[] cols1 = {"d34","g12e","pq2","m0j","e7d"};
		DBApp.createTable("fa4f", cols1);
		for(int i=0;i<41;i++)
		{
			String [] record_fa4f = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_fa4f[j] = genRandString();
			}
			DBApp.insert("fa4f", record_fa4f);
		}
		int pagesCount_fa4f = (int)Math.ceil(41.0/DBApp.dataPageSize);
		ArrayList<String> res_fa4f = new ArrayList<>();
		for(int i=0;i<pagesCount_fa4f;i++)
		{
			res_fa4f.add(i+".db");
		}
		Collections.sort(res_fa4f);
		String pagesTraceString_fa4f = "fa4f{ " + String.join(" ",res_fa4f);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_fa4f+")", fileManagerTrace1.contains(pagesTraceString_fa4f));
		//--------------------------------------------------------------------------
		String[] cols2 = {"fa","sh2","f2l","z4j"};
		DBApp.createTable("n2", cols2);
		for(int i=0;i<22;i++)
		{
			String [] record_n2 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_n2[j] = genRandString();
			}
			DBApp.insert("n2", record_n2);
		}
		int pagesCount_n2 = (int)Math.ceil(22.0/DBApp.dataPageSize);
		ArrayList<String> res_n2 = new ArrayList<>();
		for(int i=0;i<pagesCount_n2;i++)
		{
			res_n2.add(i+".db");
		}
		Collections.sort(res_n2);
		String pagesTraceString_n2 = "n2{ " + String.join(" ",res_n2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_n2+")", fileManagerTrace2.contains(pagesTraceString_n2));
		//--------------------------------------------------------------------------
		String[] cols3 = {"crpxw","fwy","vh","fz8c","y35","nl90","bu563","l6e8","k22","n8751"};
		DBApp.createTable("a11", cols3);
		for(int i=0;i<20;i++)
		{
			String [] record_a11 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_a11[j] = genRandString();
			}
			DBApp.insert("a11", record_a11);
		}
		int pagesCount_a11 = (int)Math.ceil(20.0/DBApp.dataPageSize);
		ArrayList<String> res_a11 = new ArrayList<>();
		for(int i=0;i<pagesCount_a11;i++)
		{
			res_a11.add(i+".db");
		}
		Collections.sort(res_a11);
		String pagesTraceString_a11 = "a11{ " + String.join(" ",res_a11);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_a11+")", fileManagerTrace3.contains(pagesTraceString_a11));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test58TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"na54c","nz1","r8","bq28","p63","f0r","m3y","zfn","j2"};
		DBApp.createTable("xugk", cols0);
		for(int i=0;i<60;i++)
		{
			String [] record_xugk = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_xugk[j] = genRandString();
			}
			DBApp.insert("xugk", record_xugk);
		}
		int pagesCount_xugk = (int)Math.ceil(60.0/DBApp.dataPageSize);
		ArrayList<String> res_xugk = new ArrayList<>();
		for(int i=0;i<pagesCount_xugk;i++)
		{
			res_xugk.add(i+".db");
		}
		Collections.sort(res_xugk);
		String pagesTraceString_xugk = "xugk{ " + String.join(" ",res_xugk);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_xugk+")", fileManagerTrace0.contains(pagesTraceString_xugk));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test59TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"oo4"};
		DBApp.createTable("m0a", cols0);
		for(int i=0;i<92;i++)
		{
			String [] record_m0a = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_m0a[j] = genRandString();
			}
			DBApp.insert("m0a", record_m0a);
		}
		int pagesCount_m0a = (int)Math.ceil(92.0/DBApp.dataPageSize);
		ArrayList<String> res_m0a = new ArrayList<>();
		for(int i=0;i<pagesCount_m0a;i++)
		{
			res_m0a.add(i+".db");
		}
		Collections.sort(res_m0a);
		String pagesTraceString_m0a = "m0a{ " + String.join(" ",res_m0a);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_m0a+")", fileManagerTrace0.contains(pagesTraceString_m0a));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ij8i","i0"};
		DBApp.createTable("zx506", cols1);
		for(int i=0;i<69;i++)
		{
			String [] record_zx506 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_zx506[j] = genRandString();
			}
			DBApp.insert("zx506", record_zx506);
		}
		int pagesCount_zx506 = (int)Math.ceil(69.0/DBApp.dataPageSize);
		ArrayList<String> res_zx506 = new ArrayList<>();
		for(int i=0;i<pagesCount_zx506;i++)
		{
			res_zx506.add(i+".db");
		}
		Collections.sort(res_zx506);
		String pagesTraceString_zx506 = "zx506{ " + String.join(" ",res_zx506);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_zx506+")", fileManagerTrace1.contains(pagesTraceString_zx506));
		//--------------------------------------------------------------------------
		String[] cols2 = {"y4","hneoj","qs","xdx43","gkz","rw4d","vp5j","ead","doo"};
		DBApp.createTable("q7a", cols2);
		for(int i=0;i<97;i++)
		{
			String [] record_q7a = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_q7a[j] = genRandString();
			}
			DBApp.insert("q7a", record_q7a);
		}
		int pagesCount_q7a = (int)Math.ceil(97.0/DBApp.dataPageSize);
		ArrayList<String> res_q7a = new ArrayList<>();
		for(int i=0;i<pagesCount_q7a;i++)
		{
			res_q7a.add(i+".db");
		}
		Collections.sort(res_q7a);
		String pagesTraceString_q7a = "q7a{ " + String.join(" ",res_q7a);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_q7a+")", fileManagerTrace2.contains(pagesTraceString_q7a));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test60TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"qw","aer","arrm","nwr3w","c6468","h6","w3","fv56","p79","lbn"};
		DBApp.createTable("h23", cols0);
		for(int i=0;i<88;i++)
		{
			String [] record_h23 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_h23[j] = genRandString();
			}
			DBApp.insert("h23", record_h23);
		}
		int pagesCount_h23 = (int)Math.ceil(88.0/DBApp.dataPageSize);
		ArrayList<String> res_h23 = new ArrayList<>();
		for(int i=0;i<pagesCount_h23;i++)
		{
			res_h23.add(i+".db");
		}
		Collections.sort(res_h23);
		String pagesTraceString_h23 = "h23{ " + String.join(" ",res_h23);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_h23+")", fileManagerTrace0.contains(pagesTraceString_h23));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test61TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"yqo","pxu","j5s","yj4v9","i7"};
		DBApp.createTable("vyic3", cols0);
		for(int i=0;i<59;i++)
		{
			String [] record_vyic3 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_vyic3[j] = genRandString();
			}
			DBApp.insert("vyic3", record_vyic3);
		}
		int pagesCount_vyic3 = (int)Math.ceil(59.0/DBApp.dataPageSize);
		ArrayList<String> res_vyic3 = new ArrayList<>();
		for(int i=0;i<pagesCount_vyic3;i++)
		{
			res_vyic3.add(i+".db");
		}
		Collections.sort(res_vyic3);
		String pagesTraceString_vyic3 = "vyic3{ " + String.join(" ",res_vyic3);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_vyic3+")", fileManagerTrace0.contains(pagesTraceString_vyic3));
		//--------------------------------------------------------------------------
		String[] cols1 = {"nfc4","mab9s","z772x","jn662","w81u"};
		DBApp.createTable("hr1w8", cols1);
		for(int i=0;i<85;i++)
		{
			String [] record_hr1w8 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_hr1w8[j] = genRandString();
			}
			DBApp.insert("hr1w8", record_hr1w8);
		}
		int pagesCount_hr1w8 = (int)Math.ceil(85.0/DBApp.dataPageSize);
		ArrayList<String> res_hr1w8 = new ArrayList<>();
		for(int i=0;i<pagesCount_hr1w8;i++)
		{
			res_hr1w8.add(i+".db");
		}
		Collections.sort(res_hr1w8);
		String pagesTraceString_hr1w8 = "hr1w8{ " + String.join(" ",res_hr1w8);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_hr1w8+")", fileManagerTrace1.contains(pagesTraceString_hr1w8));
		//--------------------------------------------------------------------------
		String[] cols2 = {"wcpw1","u64","lqk","ew6fw","c5s83"};
		DBApp.createTable("r6z", cols2);
		for(int i=0;i<96;i++)
		{
			String [] record_r6z = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_r6z[j] = genRandString();
			}
			DBApp.insert("r6z", record_r6z);
		}
		int pagesCount_r6z = (int)Math.ceil(96.0/DBApp.dataPageSize);
		ArrayList<String> res_r6z = new ArrayList<>();
		for(int i=0;i<pagesCount_r6z;i++)
		{
			res_r6z.add(i+".db");
		}
		Collections.sort(res_r6z);
		String pagesTraceString_r6z = "r6z{ " + String.join(" ",res_r6z);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_r6z+")", fileManagerTrace2.contains(pagesTraceString_r6z));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test62TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"ocq","kh0s","xoq","a97"};
		DBApp.createTable("yuuy", cols0);
		for(int i=0;i<84;i++)
		{
			String [] record_yuuy = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_yuuy[j] = genRandString();
			}
			DBApp.insert("yuuy", record_yuuy);
		}
		int pagesCount_yuuy = (int)Math.ceil(84.0/DBApp.dataPageSize);
		ArrayList<String> res_yuuy = new ArrayList<>();
		for(int i=0;i<pagesCount_yuuy;i++)
		{
			res_yuuy.add(i+".db");
		}
		Collections.sort(res_yuuy);
		String pagesTraceString_yuuy = "yuuy{ " + String.join(" ",res_yuuy);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_yuuy+")", fileManagerTrace0.contains(pagesTraceString_yuuy));
		//--------------------------------------------------------------------------
		String[] cols1 = {"r7","ahn","vsq","w6w88"};
		DBApp.createTable("s7y4", cols1);
		for(int i=0;i<34;i++)
		{
			String [] record_s7y4 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_s7y4[j] = genRandString();
			}
			DBApp.insert("s7y4", record_s7y4);
		}
		int pagesCount_s7y4 = (int)Math.ceil(34.0/DBApp.dataPageSize);
		ArrayList<String> res_s7y4 = new ArrayList<>();
		for(int i=0;i<pagesCount_s7y4;i++)
		{
			res_s7y4.add(i+".db");
		}
		Collections.sort(res_s7y4);
		String pagesTraceString_s7y4 = "s7y4{ " + String.join(" ",res_s7y4);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_s7y4+")", fileManagerTrace1.contains(pagesTraceString_s7y4));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test63TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"c43g"};
		DBApp.createTable("iq2", cols0);
		for(int i=0;i<86;i++)
		{
			String [] record_iq2 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_iq2[j] = genRandString();
			}
			DBApp.insert("iq2", record_iq2);
		}
		int pagesCount_iq2 = (int)Math.ceil(86.0/DBApp.dataPageSize);
		ArrayList<String> res_iq2 = new ArrayList<>();
		for(int i=0;i<pagesCount_iq2;i++)
		{
			res_iq2.add(i+".db");
		}
		Collections.sort(res_iq2);
		String pagesTraceString_iq2 = "iq2{ " + String.join(" ",res_iq2);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_iq2+")", fileManagerTrace0.contains(pagesTraceString_iq2));
		//--------------------------------------------------------------------------
		String[] cols1 = {"k61","a6u","v2","do","rj","n57","h1d","euxv","ib5j","dx7e"};
		DBApp.createTable("w0", cols1);
		for(int i=0;i<39;i++)
		{
			String [] record_w0 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_w0[j] = genRandString();
			}
			DBApp.insert("w0", record_w0);
		}
		int pagesCount_w0 = (int)Math.ceil(39.0/DBApp.dataPageSize);
		ArrayList<String> res_w0 = new ArrayList<>();
		for(int i=0;i<pagesCount_w0;i++)
		{
			res_w0.add(i+".db");
		}
		Collections.sort(res_w0);
		String pagesTraceString_w0 = "w0{ " + String.join(" ",res_w0);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_w0+")", fileManagerTrace1.contains(pagesTraceString_w0));
		//--------------------------------------------------------------------------
		String[] cols2 = {"q35x","d95","h8s","mrh85","jt","z8l","o45","k62g","ww8"};
		DBApp.createTable("o2", cols2);
		for(int i=0;i<91;i++)
		{
			String [] record_o2 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_o2[j] = genRandString();
			}
			DBApp.insert("o2", record_o2);
		}
		int pagesCount_o2 = (int)Math.ceil(91.0/DBApp.dataPageSize);
		ArrayList<String> res_o2 = new ArrayList<>();
		for(int i=0;i<pagesCount_o2;i++)
		{
			res_o2.add(i+".db");
		}
		Collections.sort(res_o2);
		String pagesTraceString_o2 = "o2{ " + String.join(" ",res_o2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_o2+")", fileManagerTrace2.contains(pagesTraceString_o2));
		//--------------------------------------------------------------------------
		String[] cols3 = {"cz8","i3","ll7","y3","gtu","a5d","cx4e"};
		DBApp.createTable("dlk", cols3);
		for(int i=0;i<28;i++)
		{
			String [] record_dlk = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_dlk[j] = genRandString();
			}
			DBApp.insert("dlk", record_dlk);
		}
		int pagesCount_dlk = (int)Math.ceil(28.0/DBApp.dataPageSize);
		ArrayList<String> res_dlk = new ArrayList<>();
		for(int i=0;i<pagesCount_dlk;i++)
		{
			res_dlk.add(i+".db");
		}
		Collections.sort(res_dlk);
		String pagesTraceString_dlk = "dlk{ " + String.join(" ",res_dlk);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_dlk+")", fileManagerTrace3.contains(pagesTraceString_dlk));
		//--------------------------------------------------------------------------
		String[] cols4 = {"n9","r1","o87","s50","or"};
		DBApp.createTable("gt22j", cols4);
		for(int i=0;i<9;i++)
		{
			String [] record_gt22j = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_gt22j[j] = genRandString();
			}
			DBApp.insert("gt22j", record_gt22j);
		}
		int pagesCount_gt22j = (int)Math.ceil(9.0/DBApp.dataPageSize);
		ArrayList<String> res_gt22j = new ArrayList<>();
		for(int i=0;i<pagesCount_gt22j;i++)
		{
			res_gt22j.add(i+".db");
		}
		Collections.sort(res_gt22j);
		String pagesTraceString_gt22j = "gt22j{ " + String.join(" ",res_gt22j);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_gt22j+")", fileManagerTrace4.contains(pagesTraceString_gt22j));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test64TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"iiff8","k2","x0x37","kc5","pn0l","f20","xta4","sld1","nk4"};
		DBApp.createTable("r7y", cols0);
		for(int i=0;i<98;i++)
		{
			String [] record_r7y = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_r7y[j] = genRandString();
			}
			DBApp.insert("r7y", record_r7y);
		}
		int pagesCount_r7y = (int)Math.ceil(98.0/DBApp.dataPageSize);
		ArrayList<String> res_r7y = new ArrayList<>();
		for(int i=0;i<pagesCount_r7y;i++)
		{
			res_r7y.add(i+".db");
		}
		Collections.sort(res_r7y);
		String pagesTraceString_r7y = "r7y{ " + String.join(" ",res_r7y);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_r7y+")", fileManagerTrace0.contains(pagesTraceString_r7y));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test65TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"ywvy","o8","a06","w63o","l1404","xt59w","j8z"};
		DBApp.createTable("qjvx", cols0);
		for(int i=0;i<80;i++)
		{
			String [] record_qjvx = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_qjvx[j] = genRandString();
			}
			DBApp.insert("qjvx", record_qjvx);
		}
		int pagesCount_qjvx = (int)Math.ceil(80.0/DBApp.dataPageSize);
		ArrayList<String> res_qjvx = new ArrayList<>();
		for(int i=0;i<pagesCount_qjvx;i++)
		{
			res_qjvx.add(i+".db");
		}
		Collections.sort(res_qjvx);
		String pagesTraceString_qjvx = "qjvx{ " + String.join(" ",res_qjvx);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_qjvx+")", fileManagerTrace0.contains(pagesTraceString_qjvx));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test66TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"nuus","a81lh","c750","vhl85","fq5","e92","x2","i596"};
		DBApp.createTable("x3", cols0);
		for(int i=0;i<90;i++)
		{
			String [] record_x3 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_x3[j] = genRandString();
			}
			DBApp.insert("x3", record_x3);
		}
		int pagesCount_x3 = (int)Math.ceil(90.0/DBApp.dataPageSize);
		ArrayList<String> res_x3 = new ArrayList<>();
		for(int i=0;i<pagesCount_x3;i++)
		{
			res_x3.add(i+".db");
		}
		Collections.sort(res_x3);
		String pagesTraceString_x3 = "x3{ " + String.join(" ",res_x3);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_x3+")", fileManagerTrace0.contains(pagesTraceString_x3));
		//--------------------------------------------------------------------------
		String[] cols1 = {"g9q8","cc"};
		DBApp.createTable("h2g", cols1);
		for(int i=0;i<3;i++)
		{
			String [] record_h2g = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_h2g[j] = genRandString();
			}
			DBApp.insert("h2g", record_h2g);
		}
		int pagesCount_h2g = (int)Math.ceil(3.0/DBApp.dataPageSize);
		ArrayList<String> res_h2g = new ArrayList<>();
		for(int i=0;i<pagesCount_h2g;i++)
		{
			res_h2g.add(i+".db");
		}
		Collections.sort(res_h2g);
		String pagesTraceString_h2g = "h2g{ " + String.join(" ",res_h2g);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_h2g+")", fileManagerTrace1.contains(pagesTraceString_h2g));
		//--------------------------------------------------------------------------
		String[] cols2 = {"s075m","ww563","mx0","wlu","wv1","r0e4","s949","z2p","mmgn","x9t"};
		DBApp.createTable("n5dsd", cols2);
		for(int i=0;i<79;i++)
		{
			String [] record_n5dsd = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_n5dsd[j] = genRandString();
			}
			DBApp.insert("n5dsd", record_n5dsd);
		}
		int pagesCount_n5dsd = (int)Math.ceil(79.0/DBApp.dataPageSize);
		ArrayList<String> res_n5dsd = new ArrayList<>();
		for(int i=0;i<pagesCount_n5dsd;i++)
		{
			res_n5dsd.add(i+".db");
		}
		Collections.sort(res_n5dsd);
		String pagesTraceString_n5dsd = "n5dsd{ " + String.join(" ",res_n5dsd);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_n5dsd+")", fileManagerTrace2.contains(pagesTraceString_n5dsd));
		//--------------------------------------------------------------------------
		String[] cols3 = {"i7","p1s","gm1","tb0","l6z","y77e"};
		DBApp.createTable("k17g", cols3);
		for(int i=0;i<80;i++)
		{
			String [] record_k17g = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_k17g[j] = genRandString();
			}
			DBApp.insert("k17g", record_k17g);
		}
		int pagesCount_k17g = (int)Math.ceil(80.0/DBApp.dataPageSize);
		ArrayList<String> res_k17g = new ArrayList<>();
		for(int i=0;i<pagesCount_k17g;i++)
		{
			res_k17g.add(i+".db");
		}
		Collections.sort(res_k17g);
		String pagesTraceString_k17g = "k17g{ " + String.join(" ",res_k17g);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_k17g+")", fileManagerTrace3.contains(pagesTraceString_k17g));
		//--------------------------------------------------------------------------
		String[] cols4 = {"u29","pyt","x86","o2fkw","ik","j75","m4bp","ja"};
		DBApp.createTable("w32v2", cols4);
		for(int i=0;i<85;i++)
		{
			String [] record_w32v2 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_w32v2[j] = genRandString();
			}
			DBApp.insert("w32v2", record_w32v2);
		}
		int pagesCount_w32v2 = (int)Math.ceil(85.0/DBApp.dataPageSize);
		ArrayList<String> res_w32v2 = new ArrayList<>();
		for(int i=0;i<pagesCount_w32v2;i++)
		{
			res_w32v2.add(i+".db");
		}
		Collections.sort(res_w32v2);
		String pagesTraceString_w32v2 = "w32v2{ " + String.join(" ",res_w32v2);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_w32v2+")", fileManagerTrace4.contains(pagesTraceString_w32v2));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test67TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"chc","es5","gy","m325","lx9ng","koas9"};
		DBApp.createTable("e761m", cols0);
		for(int i=0;i<84;i++)
		{
			String [] record_e761m = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_e761m[j] = genRandString();
			}
			DBApp.insert("e761m", record_e761m);
		}
		int pagesCount_e761m = (int)Math.ceil(84.0/DBApp.dataPageSize);
		ArrayList<String> res_e761m = new ArrayList<>();
		for(int i=0;i<pagesCount_e761m;i++)
		{
			res_e761m.add(i+".db");
		}
		Collections.sort(res_e761m);
		String pagesTraceString_e761m = "e761m{ " + String.join(" ",res_e761m);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_e761m+")", fileManagerTrace0.contains(pagesTraceString_e761m));
		//--------------------------------------------------------------------------
		String[] cols1 = {"b097","cbv","yq1qn","c7","e9s","u9t","eqvq"};
		DBApp.createTable("q885", cols1);
		for(int i=0;i<45;i++)
		{
			String [] record_q885 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_q885[j] = genRandString();
			}
			DBApp.insert("q885", record_q885);
		}
		int pagesCount_q885 = (int)Math.ceil(45.0/DBApp.dataPageSize);
		ArrayList<String> res_q885 = new ArrayList<>();
		for(int i=0;i<pagesCount_q885;i++)
		{
			res_q885.add(i+".db");
		}
		Collections.sort(res_q885);
		String pagesTraceString_q885 = "q885{ " + String.join(" ",res_q885);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_q885+")", fileManagerTrace1.contains(pagesTraceString_q885));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test68TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"q6836","xg352","xx","x1m","e6u1z","i8","vp1","yzm5","zc6"};
		DBApp.createTable("nj", cols0);
		for(int i=0;i<25;i++)
		{
			String [] record_nj = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_nj[j] = genRandString();
			}
			DBApp.insert("nj", record_nj);
		}
		int pagesCount_nj = (int)Math.ceil(25.0/DBApp.dataPageSize);
		ArrayList<String> res_nj = new ArrayList<>();
		for(int i=0;i<pagesCount_nj;i++)
		{
			res_nj.add(i+".db");
		}
		Collections.sort(res_nj);
		String pagesTraceString_nj = "nj{ " + String.join(" ",res_nj);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_nj+")", fileManagerTrace0.contains(pagesTraceString_nj));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test69TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"pfhf","fe","e8","z67","h6d","nqa","de","cg7","f1","dtdhd"};
		DBApp.createTable("s7", cols0);
		for(int i=0;i<70;i++)
		{
			String [] record_s7 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_s7[j] = genRandString();
			}
			DBApp.insert("s7", record_s7);
		}
		int pagesCount_s7 = (int)Math.ceil(70.0/DBApp.dataPageSize);
		ArrayList<String> res_s7 = new ArrayList<>();
		for(int i=0;i<pagesCount_s7;i++)
		{
			res_s7.add(i+".db");
		}
		Collections.sort(res_s7);
		String pagesTraceString_s7 = "s7{ " + String.join(" ",res_s7);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_s7+")", fileManagerTrace0.contains(pagesTraceString_s7));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test70TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"dy2y","bd8","ncx","i4zh","vky","s6r","r86","jw56","l327","g87"};
		DBApp.createTable("m2s", cols0);
		for(int i=0;i<72;i++)
		{
			String [] record_m2s = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_m2s[j] = genRandString();
			}
			DBApp.insert("m2s", record_m2s);
		}
		int pagesCount_m2s = (int)Math.ceil(72.0/DBApp.dataPageSize);
		ArrayList<String> res_m2s = new ArrayList<>();
		for(int i=0;i<pagesCount_m2s;i++)
		{
			res_m2s.add(i+".db");
		}
		Collections.sort(res_m2s);
		String pagesTraceString_m2s = "m2s{ " + String.join(" ",res_m2s);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_m2s+")", fileManagerTrace0.contains(pagesTraceString_m2s));
		//--------------------------------------------------------------------------
		String[] cols1 = {"gk9","waq","e5z7","t55x","h1r9","hj4","lm","s0tx"};
		DBApp.createTable("at", cols1);
		for(int i=0;i<52;i++)
		{
			String [] record_at = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_at[j] = genRandString();
			}
			DBApp.insert("at", record_at);
		}
		int pagesCount_at = (int)Math.ceil(52.0/DBApp.dataPageSize);
		ArrayList<String> res_at = new ArrayList<>();
		for(int i=0;i<pagesCount_at;i++)
		{
			res_at.add(i+".db");
		}
		Collections.sort(res_at);
		String pagesTraceString_at = "at{ " + String.join(" ",res_at);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_at+")", fileManagerTrace1.contains(pagesTraceString_at));
		//--------------------------------------------------------------------------
		String[] cols2 = {"bvw","p61u3","pfgt4","x8","e1i"};
		DBApp.createTable("mg1", cols2);
		for(int i=0;i<50;i++)
		{
			String [] record_mg1 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_mg1[j] = genRandString();
			}
			DBApp.insert("mg1", record_mg1);
		}
		int pagesCount_mg1 = (int)Math.ceil(50.0/DBApp.dataPageSize);
		ArrayList<String> res_mg1 = new ArrayList<>();
		for(int i=0;i<pagesCount_mg1;i++)
		{
			res_mg1.add(i+".db");
		}
		Collections.sort(res_mg1);
		String pagesTraceString_mg1 = "mg1{ " + String.join(" ",res_mg1);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_mg1+")", fileManagerTrace2.contains(pagesTraceString_mg1));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test71TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"cig1","ju","t34w7","a08j","av4c0","atpa","zqy","l22f1"};
		DBApp.createTable("k4z", cols0);
		for(int i=0;i<75;i++)
		{
			String [] record_k4z = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_k4z[j] = genRandString();
			}
			DBApp.insert("k4z", record_k4z);
		}
		int pagesCount_k4z = (int)Math.ceil(75.0/DBApp.dataPageSize);
		ArrayList<String> res_k4z = new ArrayList<>();
		for(int i=0;i<pagesCount_k4z;i++)
		{
			res_k4z.add(i+".db");
		}
		Collections.sort(res_k4z);
		String pagesTraceString_k4z = "k4z{ " + String.join(" ",res_k4z);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_k4z+")", fileManagerTrace0.contains(pagesTraceString_k4z));
		//--------------------------------------------------------------------------
		String[] cols1 = {"y10","b18i","wep","e7","ryk","o30p","d89"};
		DBApp.createTable("dx90", cols1);
		for(int i=0;i<55;i++)
		{
			String [] record_dx90 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_dx90[j] = genRandString();
			}
			DBApp.insert("dx90", record_dx90);
		}
		int pagesCount_dx90 = (int)Math.ceil(55.0/DBApp.dataPageSize);
		ArrayList<String> res_dx90 = new ArrayList<>();
		for(int i=0;i<pagesCount_dx90;i++)
		{
			res_dx90.add(i+".db");
		}
		Collections.sort(res_dx90);
		String pagesTraceString_dx90 = "dx90{ " + String.join(" ",res_dx90);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_dx90+")", fileManagerTrace1.contains(pagesTraceString_dx90));
		//--------------------------------------------------------------------------
		String[] cols2 = {"ds02","u7n","k85","q8mb","o6z","fix","cui"};
		DBApp.createTable("k90", cols2);
		for(int i=0;i<44;i++)
		{
			String [] record_k90 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_k90[j] = genRandString();
			}
			DBApp.insert("k90", record_k90);
		}
		int pagesCount_k90 = (int)Math.ceil(44.0/DBApp.dataPageSize);
		ArrayList<String> res_k90 = new ArrayList<>();
		for(int i=0;i<pagesCount_k90;i++)
		{
			res_k90.add(i+".db");
		}
		Collections.sort(res_k90);
		String pagesTraceString_k90 = "k90{ " + String.join(" ",res_k90);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_k90+")", fileManagerTrace2.contains(pagesTraceString_k90));
		//--------------------------------------------------------------------------
		String[] cols3 = {"wjs","g78","i78"};
		DBApp.createTable("eu", cols3);
		for(int i=0;i<30;i++)
		{
			String [] record_eu = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_eu[j] = genRandString();
			}
			DBApp.insert("eu", record_eu);
		}
		int pagesCount_eu = (int)Math.ceil(30.0/DBApp.dataPageSize);
		ArrayList<String> res_eu = new ArrayList<>();
		for(int i=0;i<pagesCount_eu;i++)
		{
			res_eu.add(i+".db");
		}
		Collections.sort(res_eu);
		String pagesTraceString_eu = "eu{ " + String.join(" ",res_eu);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_eu+")", fileManagerTrace3.contains(pagesTraceString_eu));
		//--------------------------------------------------------------------------
		String[] cols4 = {"x40","t37","fl1o6","t1","y9","g0","y4"};
		DBApp.createTable("t1u", cols4);
		for(int i=0;i<12;i++)
		{
			String [] record_t1u = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_t1u[j] = genRandString();
			}
			DBApp.insert("t1u", record_t1u);
		}
		int pagesCount_t1u = (int)Math.ceil(12.0/DBApp.dataPageSize);
		ArrayList<String> res_t1u = new ArrayList<>();
		for(int i=0;i<pagesCount_t1u;i++)
		{
			res_t1u.add(i+".db");
		}
		Collections.sort(res_t1u);
		String pagesTraceString_t1u = "t1u{ " + String.join(" ",res_t1u);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_t1u+")", fileManagerTrace4.contains(pagesTraceString_t1u));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test72TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"c2j9"};
		DBApp.createTable("a0", cols0);
		for(int i=0;i<4;i++)
		{
			String [] record_a0 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_a0[j] = genRandString();
			}
			DBApp.insert("a0", record_a0);
		}
		int pagesCount_a0 = (int)Math.ceil(4.0/DBApp.dataPageSize);
		ArrayList<String> res_a0 = new ArrayList<>();
		for(int i=0;i<pagesCount_a0;i++)
		{
			res_a0.add(i+".db");
		}
		Collections.sort(res_a0);
		String pagesTraceString_a0 = "a0{ " + String.join(" ",res_a0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_a0+")", fileManagerTrace0.contains(pagesTraceString_a0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"h2","xrr77","mbh","m5","alq5","ait"};
		DBApp.createTable("a3g68", cols1);
		for(int i=0;i<11;i++)
		{
			String [] record_a3g68 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_a3g68[j] = genRandString();
			}
			DBApp.insert("a3g68", record_a3g68);
		}
		int pagesCount_a3g68 = (int)Math.ceil(11.0/DBApp.dataPageSize);
		ArrayList<String> res_a3g68 = new ArrayList<>();
		for(int i=0;i<pagesCount_a3g68;i++)
		{
			res_a3g68.add(i+".db");
		}
		Collections.sort(res_a3g68);
		String pagesTraceString_a3g68 = "a3g68{ " + String.join(" ",res_a3g68);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_a3g68+")", fileManagerTrace1.contains(pagesTraceString_a3g68));
		//--------------------------------------------------------------------------
		String[] cols2 = {"n7v","y5m","e3","x3r","cbn","l364w","nj8in","a00ou","v2m53"};
		DBApp.createTable("w8ve", cols2);
		for(int i=0;i<16;i++)
		{
			String [] record_w8ve = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_w8ve[j] = genRandString();
			}
			DBApp.insert("w8ve", record_w8ve);
		}
		int pagesCount_w8ve = (int)Math.ceil(16.0/DBApp.dataPageSize);
		ArrayList<String> res_w8ve = new ArrayList<>();
		for(int i=0;i<pagesCount_w8ve;i++)
		{
			res_w8ve.add(i+".db");
		}
		Collections.sort(res_w8ve);
		String pagesTraceString_w8ve = "w8ve{ " + String.join(" ",res_w8ve);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_w8ve+")", fileManagerTrace2.contains(pagesTraceString_w8ve));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test73TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"x1n07"};
		DBApp.createTable("amd", cols0);
		for(int i=0;i<57;i++)
		{
			String [] record_amd = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_amd[j] = genRandString();
			}
			DBApp.insert("amd", record_amd);
		}
		int pagesCount_amd = (int)Math.ceil(57.0/DBApp.dataPageSize);
		ArrayList<String> res_amd = new ArrayList<>();
		for(int i=0;i<pagesCount_amd;i++)
		{
			res_amd.add(i+".db");
		}
		Collections.sort(res_amd);
		String pagesTraceString_amd = "amd{ " + String.join(" ",res_amd);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_amd+")", fileManagerTrace0.contains(pagesTraceString_amd));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test74TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"lm3","qe9tq","rry","a0q","ye1"};
		DBApp.createTable("ksr", cols0);
		for(int i=0;i<9;i++)
		{
			String [] record_ksr = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ksr[j] = genRandString();
			}
			DBApp.insert("ksr", record_ksr);
		}
		int pagesCount_ksr = (int)Math.ceil(9.0/DBApp.dataPageSize);
		ArrayList<String> res_ksr = new ArrayList<>();
		for(int i=0;i<pagesCount_ksr;i++)
		{
			res_ksr.add(i+".db");
		}
		Collections.sort(res_ksr);
		String pagesTraceString_ksr = "ksr{ " + String.join(" ",res_ksr);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ksr+")", fileManagerTrace0.contains(pagesTraceString_ksr));
		//--------------------------------------------------------------------------
		String[] cols1 = {"i71cw","w9","vitl","xn4x0","g3v7","o130","r3n","mn5","k0"};
		DBApp.createTable("uy8", cols1);
		for(int i=0;i<29;i++)
		{
			String [] record_uy8 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_uy8[j] = genRandString();
			}
			DBApp.insert("uy8", record_uy8);
		}
		int pagesCount_uy8 = (int)Math.ceil(29.0/DBApp.dataPageSize);
		ArrayList<String> res_uy8 = new ArrayList<>();
		for(int i=0;i<pagesCount_uy8;i++)
		{
			res_uy8.add(i+".db");
		}
		Collections.sort(res_uy8);
		String pagesTraceString_uy8 = "uy8{ " + String.join(" ",res_uy8);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_uy8+")", fileManagerTrace1.contains(pagesTraceString_uy8));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test75TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"og1","ar8","ak9","i7","hip"};
		DBApp.createTable("z11", cols0);
		for(int i=0;i<52;i++)
		{
			String [] record_z11 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_z11[j] = genRandString();
			}
			DBApp.insert("z11", record_z11);
		}
		int pagesCount_z11 = (int)Math.ceil(52.0/DBApp.dataPageSize);
		ArrayList<String> res_z11 = new ArrayList<>();
		for(int i=0;i<pagesCount_z11;i++)
		{
			res_z11.add(i+".db");
		}
		Collections.sort(res_z11);
		String pagesTraceString_z11 = "z11{ " + String.join(" ",res_z11);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_z11+")", fileManagerTrace0.contains(pagesTraceString_z11));
		//--------------------------------------------------------------------------
		String[] cols1 = {"pmc","qk"};
		DBApp.createTable("mj", cols1);
		for(int i=0;i<51;i++)
		{
			String [] record_mj = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_mj[j] = genRandString();
			}
			DBApp.insert("mj", record_mj);
		}
		int pagesCount_mj = (int)Math.ceil(51.0/DBApp.dataPageSize);
		ArrayList<String> res_mj = new ArrayList<>();
		for(int i=0;i<pagesCount_mj;i++)
		{
			res_mj.add(i+".db");
		}
		Collections.sort(res_mj);
		String pagesTraceString_mj = "mj{ " + String.join(" ",res_mj);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_mj+")", fileManagerTrace1.contains(pagesTraceString_mj));
		//--------------------------------------------------------------------------
		String[] cols2 = {"xty","m666","uiok","u7m4","arg","y21ni","ls84i","f1sv2"};
		DBApp.createTable("m14", cols2);
		for(int i=0;i<25;i++)
		{
			String [] record_m14 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_m14[j] = genRandString();
			}
			DBApp.insert("m14", record_m14);
		}
		int pagesCount_m14 = (int)Math.ceil(25.0/DBApp.dataPageSize);
		ArrayList<String> res_m14 = new ArrayList<>();
		for(int i=0;i<pagesCount_m14;i++)
		{
			res_m14.add(i+".db");
		}
		Collections.sort(res_m14);
		String pagesTraceString_m14 = "m14{ " + String.join(" ",res_m14);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_m14+")", fileManagerTrace2.contains(pagesTraceString_m14));
		//--------------------------------------------------------------------------
		String[] cols3 = {"xbl","v4n","wet","mxr","hdx","q2a","sc07","k9p","jw0","qow"};
		DBApp.createTable("mogf", cols3);
		for(int i=0;i<71;i++)
		{
			String [] record_mogf = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_mogf[j] = genRandString();
			}
			DBApp.insert("mogf", record_mogf);
		}
		int pagesCount_mogf = (int)Math.ceil(71.0/DBApp.dataPageSize);
		ArrayList<String> res_mogf = new ArrayList<>();
		for(int i=0;i<pagesCount_mogf;i++)
		{
			res_mogf.add(i+".db");
		}
		Collections.sort(res_mogf);
		String pagesTraceString_mogf = "mogf{ " + String.join(" ",res_mogf);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_mogf+")", fileManagerTrace3.contains(pagesTraceString_mogf));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test76TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"yke","l638o","mp7","p47","r06d","mjte","kxq"};
		DBApp.createTable("zu", cols0);
		for(int i=0;i<96;i++)
		{
			String [] record_zu = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_zu[j] = genRandString();
			}
			DBApp.insert("zu", record_zu);
		}
		int pagesCount_zu = (int)Math.ceil(96.0/DBApp.dataPageSize);
		ArrayList<String> res_zu = new ArrayList<>();
		for(int i=0;i<pagesCount_zu;i++)
		{
			res_zu.add(i+".db");
		}
		Collections.sort(res_zu);
		String pagesTraceString_zu = "zu{ " + String.join(" ",res_zu);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_zu+")", fileManagerTrace0.contains(pagesTraceString_zu));
		//--------------------------------------------------------------------------
		String[] cols1 = {"w3ow","mi7","bj7","yfu73"};
		DBApp.createTable("g1x", cols1);
		for(int i=0;i<50;i++)
		{
			String [] record_g1x = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_g1x[j] = genRandString();
			}
			DBApp.insert("g1x", record_g1x);
		}
		int pagesCount_g1x = (int)Math.ceil(50.0/DBApp.dataPageSize);
		ArrayList<String> res_g1x = new ArrayList<>();
		for(int i=0;i<pagesCount_g1x;i++)
		{
			res_g1x.add(i+".db");
		}
		Collections.sort(res_g1x);
		String pagesTraceString_g1x = "g1x{ " + String.join(" ",res_g1x);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_g1x+")", fileManagerTrace1.contains(pagesTraceString_g1x));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test77TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"e7","ql7","ete","e3","f6mm6"};
		DBApp.createTable("l0", cols0);
		for(int i=0;i<89;i++)
		{
			String [] record_l0 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_l0[j] = genRandString();
			}
			DBApp.insert("l0", record_l0);
		}
		int pagesCount_l0 = (int)Math.ceil(89.0/DBApp.dataPageSize);
		ArrayList<String> res_l0 = new ArrayList<>();
		for(int i=0;i<pagesCount_l0;i++)
		{
			res_l0.add(i+".db");
		}
		Collections.sort(res_l0);
		String pagesTraceString_l0 = "l0{ " + String.join(" ",res_l0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_l0+")", fileManagerTrace0.contains(pagesTraceString_l0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"x46zu","koh97","rl9x","io5n","h30a","dc","mb6","b10hu","xl2f","yydp5"};
		DBApp.createTable("nls21", cols1);
		for(int i=0;i<43;i++)
		{
			String [] record_nls21 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_nls21[j] = genRandString();
			}
			DBApp.insert("nls21", record_nls21);
		}
		int pagesCount_nls21 = (int)Math.ceil(43.0/DBApp.dataPageSize);
		ArrayList<String> res_nls21 = new ArrayList<>();
		for(int i=0;i<pagesCount_nls21;i++)
		{
			res_nls21.add(i+".db");
		}
		Collections.sort(res_nls21);
		String pagesTraceString_nls21 = "nls21{ " + String.join(" ",res_nls21);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_nls21+")", fileManagerTrace1.contains(pagesTraceString_nls21));
		//--------------------------------------------------------------------------
		String[] cols2 = {"vx8"};
		DBApp.createTable("g1", cols2);
		for(int i=0;i<81;i++)
		{
			String [] record_g1 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_g1[j] = genRandString();
			}
			DBApp.insert("g1", record_g1);
		}
		int pagesCount_g1 = (int)Math.ceil(81.0/DBApp.dataPageSize);
		ArrayList<String> res_g1 = new ArrayList<>();
		for(int i=0;i<pagesCount_g1;i++)
		{
			res_g1.add(i+".db");
		}
		Collections.sort(res_g1);
		String pagesTraceString_g1 = "g1{ " + String.join(" ",res_g1);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_g1+")", fileManagerTrace2.contains(pagesTraceString_g1));
		//--------------------------------------------------------------------------
		String[] cols3 = {"m7p","e0v5r","v3"};
		DBApp.createTable("ie", cols3);
		for(int i=0;i<82;i++)
		{
			String [] record_ie = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_ie[j] = genRandString();
			}
			DBApp.insert("ie", record_ie);
		}
		int pagesCount_ie = (int)Math.ceil(82.0/DBApp.dataPageSize);
		ArrayList<String> res_ie = new ArrayList<>();
		for(int i=0;i<pagesCount_ie;i++)
		{
			res_ie.add(i+".db");
		}
		Collections.sort(res_ie);
		String pagesTraceString_ie = "ie{ " + String.join(" ",res_ie);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ie+")", fileManagerTrace3.contains(pagesTraceString_ie));
		//--------------------------------------------------------------------------
		String[] cols4 = {"piv","e2","u4d","cah","kn","o61","aa5","z9","ch55o"};
		DBApp.createTable("m776", cols4);
		for(int i=0;i<75;i++)
		{
			String [] record_m776 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_m776[j] = genRandString();
			}
			DBApp.insert("m776", record_m776);
		}
		int pagesCount_m776 = (int)Math.ceil(75.0/DBApp.dataPageSize);
		ArrayList<String> res_m776 = new ArrayList<>();
		for(int i=0;i<pagesCount_m776;i++)
		{
			res_m776.add(i+".db");
		}
		Collections.sort(res_m776);
		String pagesTraceString_m776 = "m776{ " + String.join(" ",res_m776);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_m776+")", fileManagerTrace4.contains(pagesTraceString_m776));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test78TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"y7k","nt8x2","t23lu","ix4","h48p"};
		DBApp.createTable("q393", cols0);
		for(int i=0;i<86;i++)
		{
			String [] record_q393 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_q393[j] = genRandString();
			}
			DBApp.insert("q393", record_q393);
		}
		int pagesCount_q393 = (int)Math.ceil(86.0/DBApp.dataPageSize);
		ArrayList<String> res_q393 = new ArrayList<>();
		for(int i=0;i<pagesCount_q393;i++)
		{
			res_q393.add(i+".db");
		}
		Collections.sort(res_q393);
		String pagesTraceString_q393 = "q393{ " + String.join(" ",res_q393);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_q393+")", fileManagerTrace0.contains(pagesTraceString_q393));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test79TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"ks","bj1","x8","h718","k1m","x9x","gx9q"};
		DBApp.createTable("qj", cols0);
		for(int i=0;i<17;i++)
		{
			String [] record_qj = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_qj[j] = genRandString();
			}
			DBApp.insert("qj", record_qj);
		}
		int pagesCount_qj = (int)Math.ceil(17.0/DBApp.dataPageSize);
		ArrayList<String> res_qj = new ArrayList<>();
		for(int i=0;i<pagesCount_qj;i++)
		{
			res_qj.add(i+".db");
		}
		Collections.sort(res_qj);
		String pagesTraceString_qj = "qj{ " + String.join(" ",res_qj);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_qj+")", fileManagerTrace0.contains(pagesTraceString_qj));
		//--------------------------------------------------------------------------
		String[] cols1 = {"kco2","egavd","or4","u42v"};
		DBApp.createTable("e81", cols1);
		for(int i=0;i<53;i++)
		{
			String [] record_e81 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_e81[j] = genRandString();
			}
			DBApp.insert("e81", record_e81);
		}
		int pagesCount_e81 = (int)Math.ceil(53.0/DBApp.dataPageSize);
		ArrayList<String> res_e81 = new ArrayList<>();
		for(int i=0;i<pagesCount_e81;i++)
		{
			res_e81.add(i+".db");
		}
		Collections.sort(res_e81);
		String pagesTraceString_e81 = "e81{ " + String.join(" ",res_e81);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_e81+")", fileManagerTrace1.contains(pagesTraceString_e81));
		//--------------------------------------------------------------------------
		String[] cols2 = {"e5","q43","y4y0v","fd"};
		DBApp.createTable("mwp", cols2);
		for(int i=0;i<52;i++)
		{
			String [] record_mwp = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_mwp[j] = genRandString();
			}
			DBApp.insert("mwp", record_mwp);
		}
		int pagesCount_mwp = (int)Math.ceil(52.0/DBApp.dataPageSize);
		ArrayList<String> res_mwp = new ArrayList<>();
		for(int i=0;i<pagesCount_mwp;i++)
		{
			res_mwp.add(i+".db");
		}
		Collections.sort(res_mwp);
		String pagesTraceString_mwp = "mwp{ " + String.join(" ",res_mwp);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_mwp+")", fileManagerTrace2.contains(pagesTraceString_mwp));
		//--------------------------------------------------------------------------
		String[] cols3 = {"wtj","x91"};
		DBApp.createTable("j3fm", cols3);
		for(int i=0;i<23;i++)
		{
			String [] record_j3fm = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_j3fm[j] = genRandString();
			}
			DBApp.insert("j3fm", record_j3fm);
		}
		int pagesCount_j3fm = (int)Math.ceil(23.0/DBApp.dataPageSize);
		ArrayList<String> res_j3fm = new ArrayList<>();
		for(int i=0;i<pagesCount_j3fm;i++)
		{
			res_j3fm.add(i+".db");
		}
		Collections.sort(res_j3fm);
		String pagesTraceString_j3fm = "j3fm{ " + String.join(" ",res_j3fm);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_j3fm+")", fileManagerTrace3.contains(pagesTraceString_j3fm));
		//--------------------------------------------------------------------------
		String[] cols4 = {"y8456","a60b","n2yhd"};
		DBApp.createTable("gq88t", cols4);
		for(int i=0;i<87;i++)
		{
			String [] record_gq88t = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_gq88t[j] = genRandString();
			}
			DBApp.insert("gq88t", record_gq88t);
		}
		int pagesCount_gq88t = (int)Math.ceil(87.0/DBApp.dataPageSize);
		ArrayList<String> res_gq88t = new ArrayList<>();
		for(int i=0;i<pagesCount_gq88t;i++)
		{
			res_gq88t.add(i+".db");
		}
		Collections.sort(res_gq88t);
		String pagesTraceString_gq88t = "gq88t{ " + String.join(" ",res_gq88t);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_gq88t+")", fileManagerTrace4.contains(pagesTraceString_gq88t));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test80TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"s73","gose","gmj","a3v","u24","nu","k1fx0","d3"};
		DBApp.createTable("n84b5", cols0);
		for(int i=0;i<13;i++)
		{
			String [] record_n84b5 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_n84b5[j] = genRandString();
			}
			DBApp.insert("n84b5", record_n84b5);
		}
		int pagesCount_n84b5 = (int)Math.ceil(13.0/DBApp.dataPageSize);
		ArrayList<String> res_n84b5 = new ArrayList<>();
		for(int i=0;i<pagesCount_n84b5;i++)
		{
			res_n84b5.add(i+".db");
		}
		Collections.sort(res_n84b5);
		String pagesTraceString_n84b5 = "n84b5{ " + String.join(" ",res_n84b5);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_n84b5+")", fileManagerTrace0.contains(pagesTraceString_n84b5));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ov4","v5","mausb","y57d","ch5","fsi7","kym8","knq"};
		DBApp.createTable("z0s0", cols1);
		for(int i=0;i<90;i++)
		{
			String [] record_z0s0 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_z0s0[j] = genRandString();
			}
			DBApp.insert("z0s0", record_z0s0);
		}
		int pagesCount_z0s0 = (int)Math.ceil(90.0/DBApp.dataPageSize);
		ArrayList<String> res_z0s0 = new ArrayList<>();
		for(int i=0;i<pagesCount_z0s0;i++)
		{
			res_z0s0.add(i+".db");
		}
		Collections.sort(res_z0s0);
		String pagesTraceString_z0s0 = "z0s0{ " + String.join(" ",res_z0s0);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_z0s0+")", fileManagerTrace1.contains(pagesTraceString_z0s0));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test81TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"cjmf","z3a","r64","d7g","os3","e5","gdd3","bgg"};
		DBApp.createTable("qy", cols0);
		for(int i=0;i<77;i++)
		{
			String [] record_qy = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_qy[j] = genRandString();
			}
			DBApp.insert("qy", record_qy);
		}
		int pagesCount_qy = (int)Math.ceil(77.0/DBApp.dataPageSize);
		ArrayList<String> res_qy = new ArrayList<>();
		for(int i=0;i<pagesCount_qy;i++)
		{
			res_qy.add(i+".db");
		}
		Collections.sort(res_qy);
		String pagesTraceString_qy = "qy{ " + String.join(" ",res_qy);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_qy+")", fileManagerTrace0.contains(pagesTraceString_qy));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test82TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"fnx","cu057","gca6","eix25","k77","q9vdf","v3"};
		DBApp.createTable("kw8hw", cols0);
		for(int i=0;i<76;i++)
		{
			String [] record_kw8hw = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_kw8hw[j] = genRandString();
			}
			DBApp.insert("kw8hw", record_kw8hw);
		}
		int pagesCount_kw8hw = (int)Math.ceil(76.0/DBApp.dataPageSize);
		ArrayList<String> res_kw8hw = new ArrayList<>();
		for(int i=0;i<pagesCount_kw8hw;i++)
		{
			res_kw8hw.add(i+".db");
		}
		Collections.sort(res_kw8hw);
		String pagesTraceString_kw8hw = "kw8hw{ " + String.join(" ",res_kw8hw);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_kw8hw+")", fileManagerTrace0.contains(pagesTraceString_kw8hw));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ag3","qt3v1"};
		DBApp.createTable("he", cols1);
		for(int i=0;i<70;i++)
		{
			String [] record_he = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_he[j] = genRandString();
			}
			DBApp.insert("he", record_he);
		}
		int pagesCount_he = (int)Math.ceil(70.0/DBApp.dataPageSize);
		ArrayList<String> res_he = new ArrayList<>();
		for(int i=0;i<pagesCount_he;i++)
		{
			res_he.add(i+".db");
		}
		Collections.sort(res_he);
		String pagesTraceString_he = "he{ " + String.join(" ",res_he);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_he+")", fileManagerTrace1.contains(pagesTraceString_he));
		//--------------------------------------------------------------------------
		String[] cols2 = {"g1s1","tl5s","h0z","yf29","t3","x9w","o7463","q0zv","yay","c9v"};
		DBApp.createTable("km3", cols2);
		for(int i=0;i<48;i++)
		{
			String [] record_km3 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_km3[j] = genRandString();
			}
			DBApp.insert("km3", record_km3);
		}
		int pagesCount_km3 = (int)Math.ceil(48.0/DBApp.dataPageSize);
		ArrayList<String> res_km3 = new ArrayList<>();
		for(int i=0;i<pagesCount_km3;i++)
		{
			res_km3.add(i+".db");
		}
		Collections.sort(res_km3);
		String pagesTraceString_km3 = "km3{ " + String.join(" ",res_km3);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_km3+")", fileManagerTrace2.contains(pagesTraceString_km3));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test83TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"um4","vqfa","w9s8k","ky97","by","c3","u4r","tv","cc4z"};
		DBApp.createTable("i55q", cols0);
		for(int i=0;i<97;i++)
		{
			String [] record_i55q = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_i55q[j] = genRandString();
			}
			DBApp.insert("i55q", record_i55q);
		}
		int pagesCount_i55q = (int)Math.ceil(97.0/DBApp.dataPageSize);
		ArrayList<String> res_i55q = new ArrayList<>();
		for(int i=0;i<pagesCount_i55q;i++)
		{
			res_i55q.add(i+".db");
		}
		Collections.sort(res_i55q);
		String pagesTraceString_i55q = "i55q{ " + String.join(" ",res_i55q);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_i55q+")", fileManagerTrace0.contains(pagesTraceString_i55q));
		//--------------------------------------------------------------------------
		String[] cols1 = {"op5","f88h5","d3"};
		DBApp.createTable("q5ypd", cols1);
		for(int i=0;i<75;i++)
		{
			String [] record_q5ypd = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_q5ypd[j] = genRandString();
			}
			DBApp.insert("q5ypd", record_q5ypd);
		}
		int pagesCount_q5ypd = (int)Math.ceil(75.0/DBApp.dataPageSize);
		ArrayList<String> res_q5ypd = new ArrayList<>();
		for(int i=0;i<pagesCount_q5ypd;i++)
		{
			res_q5ypd.add(i+".db");
		}
		Collections.sort(res_q5ypd);
		String pagesTraceString_q5ypd = "q5ypd{ " + String.join(" ",res_q5ypd);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_q5ypd+")", fileManagerTrace1.contains(pagesTraceString_q5ypd));
		//--------------------------------------------------------------------------
		String[] cols2 = {"ic4m","o90m","jsy","r6ez","tvs","f445","xw0","b8d","o26"};
		DBApp.createTable("ffad", cols2);
		for(int i=0;i<95;i++)
		{
			String [] record_ffad = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_ffad[j] = genRandString();
			}
			DBApp.insert("ffad", record_ffad);
		}
		int pagesCount_ffad = (int)Math.ceil(95.0/DBApp.dataPageSize);
		ArrayList<String> res_ffad = new ArrayList<>();
		for(int i=0;i<pagesCount_ffad;i++)
		{
			res_ffad.add(i+".db");
		}
		Collections.sort(res_ffad);
		String pagesTraceString_ffad = "ffad{ " + String.join(" ",res_ffad);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ffad+")", fileManagerTrace2.contains(pagesTraceString_ffad));
		//--------------------------------------------------------------------------
		String[] cols3 = {"u9t1","a01","t7p","ns"};
		DBApp.createTable("e6", cols3);
		for(int i=0;i<36;i++)
		{
			String [] record_e6 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_e6[j] = genRandString();
			}
			DBApp.insert("e6", record_e6);
		}
		int pagesCount_e6 = (int)Math.ceil(36.0/DBApp.dataPageSize);
		ArrayList<String> res_e6 = new ArrayList<>();
		for(int i=0;i<pagesCount_e6;i++)
		{
			res_e6.add(i+".db");
		}
		Collections.sort(res_e6);
		String pagesTraceString_e6 = "e6{ " + String.join(" ",res_e6);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_e6+")", fileManagerTrace3.contains(pagesTraceString_e6));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test84TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"yde","zs","jn5","f9b","ad1","e8i9d","ezt","c81"};
		DBApp.createTable("j36", cols0);
		for(int i=0;i<97;i++)
		{
			String [] record_j36 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_j36[j] = genRandString();
			}
			DBApp.insert("j36", record_j36);
		}
		int pagesCount_j36 = (int)Math.ceil(97.0/DBApp.dataPageSize);
		ArrayList<String> res_j36 = new ArrayList<>();
		for(int i=0;i<pagesCount_j36;i++)
		{
			res_j36.add(i+".db");
		}
		Collections.sort(res_j36);
		String pagesTraceString_j36 = "j36{ " + String.join(" ",res_j36);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_j36+")", fileManagerTrace0.contains(pagesTraceString_j36));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test85TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"u0c","er0g","va7ex","k03a0","zer"};
		DBApp.createTable("m6g", cols0);
		for(int i=0;i<81;i++)
		{
			String [] record_m6g = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_m6g[j] = genRandString();
			}
			DBApp.insert("m6g", record_m6g);
		}
		int pagesCount_m6g = (int)Math.ceil(81.0/DBApp.dataPageSize);
		ArrayList<String> res_m6g = new ArrayList<>();
		for(int i=0;i<pagesCount_m6g;i++)
		{
			res_m6g.add(i+".db");
		}
		Collections.sort(res_m6g);
		String pagesTraceString_m6g = "m6g{ " + String.join(" ",res_m6g);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_m6g+")", fileManagerTrace0.contains(pagesTraceString_m6g));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ik","k1eu","diu7","cpa","q144","l4","wdt","xi75j","t97","c86"};
		DBApp.createTable("wi9", cols1);
		for(int i=0;i<24;i++)
		{
			String [] record_wi9 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_wi9[j] = genRandString();
			}
			DBApp.insert("wi9", record_wi9);
		}
		int pagesCount_wi9 = (int)Math.ceil(24.0/DBApp.dataPageSize);
		ArrayList<String> res_wi9 = new ArrayList<>();
		for(int i=0;i<pagesCount_wi9;i++)
		{
			res_wi9.add(i+".db");
		}
		Collections.sort(res_wi9);
		String pagesTraceString_wi9 = "wi9{ " + String.join(" ",res_wi9);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_wi9+")", fileManagerTrace1.contains(pagesTraceString_wi9));
		//--------------------------------------------------------------------------
		String[] cols2 = {"ap58","y41"};
		DBApp.createTable("e16bk", cols2);
		for(int i=0;i<48;i++)
		{
			String [] record_e16bk = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_e16bk[j] = genRandString();
			}
			DBApp.insert("e16bk", record_e16bk);
		}
		int pagesCount_e16bk = (int)Math.ceil(48.0/DBApp.dataPageSize);
		ArrayList<String> res_e16bk = new ArrayList<>();
		for(int i=0;i<pagesCount_e16bk;i++)
		{
			res_e16bk.add(i+".db");
		}
		Collections.sort(res_e16bk);
		String pagesTraceString_e16bk = "e16bk{ " + String.join(" ",res_e16bk);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_e16bk+")", fileManagerTrace2.contains(pagesTraceString_e16bk));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test86TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"qo7","yr","u32h7","x009","yg960"};
		DBApp.createTable("iews1", cols0);
		for(int i=0;i<79;i++)
		{
			String [] record_iews1 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_iews1[j] = genRandString();
			}
			DBApp.insert("iews1", record_iews1);
		}
		int pagesCount_iews1 = (int)Math.ceil(79.0/DBApp.dataPageSize);
		ArrayList<String> res_iews1 = new ArrayList<>();
		for(int i=0;i<pagesCount_iews1;i++)
		{
			res_iews1.add(i+".db");
		}
		Collections.sort(res_iews1);
		String pagesTraceString_iews1 = "iews1{ " + String.join(" ",res_iews1);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_iews1+")", fileManagerTrace0.contains(pagesTraceString_iews1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"z5","mw9jy","f70","k1u08","o482","f9t6","ek3","pz678","eo9"};
		DBApp.createTable("ac60e", cols1);
		for(int i=0;i<77;i++)
		{
			String [] record_ac60e = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_ac60e[j] = genRandString();
			}
			DBApp.insert("ac60e", record_ac60e);
		}
		int pagesCount_ac60e = (int)Math.ceil(77.0/DBApp.dataPageSize);
		ArrayList<String> res_ac60e = new ArrayList<>();
		for(int i=0;i<pagesCount_ac60e;i++)
		{
			res_ac60e.add(i+".db");
		}
		Collections.sort(res_ac60e);
		String pagesTraceString_ac60e = "ac60e{ " + String.join(" ",res_ac60e);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ac60e+")", fileManagerTrace1.contains(pagesTraceString_ac60e));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test87TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"ezq2","v6k58","d30","m927"};
		DBApp.createTable("n4", cols0);
		for(int i=0;i<61;i++)
		{
			String [] record_n4 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_n4[j] = genRandString();
			}
			DBApp.insert("n4", record_n4);
		}
		int pagesCount_n4 = (int)Math.ceil(61.0/DBApp.dataPageSize);
		ArrayList<String> res_n4 = new ArrayList<>();
		for(int i=0;i<pagesCount_n4;i++)
		{
			res_n4.add(i+".db");
		}
		Collections.sort(res_n4);
		String pagesTraceString_n4 = "n4{ " + String.join(" ",res_n4);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_n4+")", fileManagerTrace0.contains(pagesTraceString_n4));
		//--------------------------------------------------------------------------
		String[] cols1 = {"maec6","c9s","z7i7","b44","p3"};
		DBApp.createTable("vsfg", cols1);
		for(int i=0;i<79;i++)
		{
			String [] record_vsfg = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_vsfg[j] = genRandString();
			}
			DBApp.insert("vsfg", record_vsfg);
		}
		int pagesCount_vsfg = (int)Math.ceil(79.0/DBApp.dataPageSize);
		ArrayList<String> res_vsfg = new ArrayList<>();
		for(int i=0;i<pagesCount_vsfg;i++)
		{
			res_vsfg.add(i+".db");
		}
		Collections.sort(res_vsfg);
		String pagesTraceString_vsfg = "vsfg{ " + String.join(" ",res_vsfg);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_vsfg+")", fileManagerTrace1.contains(pagesTraceString_vsfg));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test88TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"h5x","aliw","x703l","op6","i78","i416","fft1","o3i"};
		DBApp.createTable("g2moj", cols0);
		for(int i=0;i<27;i++)
		{
			String [] record_g2moj = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_g2moj[j] = genRandString();
			}
			DBApp.insert("g2moj", record_g2moj);
		}
		int pagesCount_g2moj = (int)Math.ceil(27.0/DBApp.dataPageSize);
		ArrayList<String> res_g2moj = new ArrayList<>();
		for(int i=0;i<pagesCount_g2moj;i++)
		{
			res_g2moj.add(i+".db");
		}
		Collections.sort(res_g2moj);
		String pagesTraceString_g2moj = "g2moj{ " + String.join(" ",res_g2moj);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_g2moj+")", fileManagerTrace0.contains(pagesTraceString_g2moj));
		//--------------------------------------------------------------------------
		String[] cols1 = {"t007s","w3p","y2m"};
		DBApp.createTable("fm", cols1);
		for(int i=0;i<49;i++)
		{
			String [] record_fm = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_fm[j] = genRandString();
			}
			DBApp.insert("fm", record_fm);
		}
		int pagesCount_fm = (int)Math.ceil(49.0/DBApp.dataPageSize);
		ArrayList<String> res_fm = new ArrayList<>();
		for(int i=0;i<pagesCount_fm;i++)
		{
			res_fm.add(i+".db");
		}
		Collections.sort(res_fm);
		String pagesTraceString_fm = "fm{ " + String.join(" ",res_fm);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_fm+")", fileManagerTrace1.contains(pagesTraceString_fm));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test89TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"qg8","g88c","klt2","p7a","s93","ro13q","fd02i","f3o"};
		DBApp.createTable("hem", cols0);
		for(int i=0;i<18;i++)
		{
			String [] record_hem = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_hem[j] = genRandString();
			}
			DBApp.insert("hem", record_hem);
		}
		int pagesCount_hem = (int)Math.ceil(18.0/DBApp.dataPageSize);
		ArrayList<String> res_hem = new ArrayList<>();
		for(int i=0;i<pagesCount_hem;i++)
		{
			res_hem.add(i+".db");
		}
		Collections.sort(res_hem);
		String pagesTraceString_hem = "hem{ " + String.join(" ",res_hem);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_hem+")", fileManagerTrace0.contains(pagesTraceString_hem));
		//--------------------------------------------------------------------------
		String[] cols1 = {"xcw","j22xb","y79","x0533","ls54","fp91","yq61","nckd","a21","lhp"};
		DBApp.createTable("xvz6g", cols1);
		for(int i=0;i<19;i++)
		{
			String [] record_xvz6g = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_xvz6g[j] = genRandString();
			}
			DBApp.insert("xvz6g", record_xvz6g);
		}
		int pagesCount_xvz6g = (int)Math.ceil(19.0/DBApp.dataPageSize);
		ArrayList<String> res_xvz6g = new ArrayList<>();
		for(int i=0;i<pagesCount_xvz6g;i++)
		{
			res_xvz6g.add(i+".db");
		}
		Collections.sort(res_xvz6g);
		String pagesTraceString_xvz6g = "xvz6g{ " + String.join(" ",res_xvz6g);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_xvz6g+")", fileManagerTrace1.contains(pagesTraceString_xvz6g));
		//--------------------------------------------------------------------------
		String[] cols2 = {"p26z2","t133q","bp","n3j","w19","z5ka4"};
		DBApp.createTable("y186", cols2);
		for(int i=0;i<44;i++)
		{
			String [] record_y186 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_y186[j] = genRandString();
			}
			DBApp.insert("y186", record_y186);
		}
		int pagesCount_y186 = (int)Math.ceil(44.0/DBApp.dataPageSize);
		ArrayList<String> res_y186 = new ArrayList<>();
		for(int i=0;i<pagesCount_y186;i++)
		{
			res_y186.add(i+".db");
		}
		Collections.sort(res_y186);
		String pagesTraceString_y186 = "y186{ " + String.join(" ",res_y186);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_y186+")", fileManagerTrace2.contains(pagesTraceString_y186));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test90TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"n6jd","tg"};
		DBApp.createTable("vify", cols0);
		for(int i=0;i<69;i++)
		{
			String [] record_vify = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_vify[j] = genRandString();
			}
			DBApp.insert("vify", record_vify);
		}
		int pagesCount_vify = (int)Math.ceil(69.0/DBApp.dataPageSize);
		ArrayList<String> res_vify = new ArrayList<>();
		for(int i=0;i<pagesCount_vify;i++)
		{
			res_vify.add(i+".db");
		}
		Collections.sort(res_vify);
		String pagesTraceString_vify = "vify{ " + String.join(" ",res_vify);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_vify+")", fileManagerTrace0.contains(pagesTraceString_vify));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ou","q0"};
		DBApp.createTable("pi", cols1);
		for(int i=0;i<55;i++)
		{
			String [] record_pi = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_pi[j] = genRandString();
			}
			DBApp.insert("pi", record_pi);
		}
		int pagesCount_pi = (int)Math.ceil(55.0/DBApp.dataPageSize);
		ArrayList<String> res_pi = new ArrayList<>();
		for(int i=0;i<pagesCount_pi;i++)
		{
			res_pi.add(i+".db");
		}
		Collections.sort(res_pi);
		String pagesTraceString_pi = "pi{ " + String.join(" ",res_pi);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_pi+")", fileManagerTrace1.contains(pagesTraceString_pi));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test91TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"v1iom","s93","m9a","oq3","v0","toqgt","zis"};
		DBApp.createTable("i8t", cols0);
		for(int i=0;i<37;i++)
		{
			String [] record_i8t = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_i8t[j] = genRandString();
			}
			DBApp.insert("i8t", record_i8t);
		}
		int pagesCount_i8t = (int)Math.ceil(37.0/DBApp.dataPageSize);
		ArrayList<String> res_i8t = new ArrayList<>();
		for(int i=0;i<pagesCount_i8t;i++)
		{
			res_i8t.add(i+".db");
		}
		Collections.sort(res_i8t);
		String pagesTraceString_i8t = "i8t{ " + String.join(" ",res_i8t);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_i8t+")", fileManagerTrace0.contains(pagesTraceString_i8t));
		//--------------------------------------------------------------------------
		String[] cols1 = {"mn0","zca","n8g3i","fun","gy93","k8","l04","rw","t0e"};
		DBApp.createTable("j611", cols1);
		for(int i=0;i<98;i++)
		{
			String [] record_j611 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_j611[j] = genRandString();
			}
			DBApp.insert("j611", record_j611);
		}
		int pagesCount_j611 = (int)Math.ceil(98.0/DBApp.dataPageSize);
		ArrayList<String> res_j611 = new ArrayList<>();
		for(int i=0;i<pagesCount_j611;i++)
		{
			res_j611.add(i+".db");
		}
		Collections.sort(res_j611);
		String pagesTraceString_j611 = "j611{ " + String.join(" ",res_j611);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_j611+")", fileManagerTrace1.contains(pagesTraceString_j611));
		//--------------------------------------------------------------------------
		String[] cols2 = {"jowm","h13","v4","kgp","dg8"};
		DBApp.createTable("jqz", cols2);
		for(int i=0;i<75;i++)
		{
			String [] record_jqz = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_jqz[j] = genRandString();
			}
			DBApp.insert("jqz", record_jqz);
		}
		int pagesCount_jqz = (int)Math.ceil(75.0/DBApp.dataPageSize);
		ArrayList<String> res_jqz = new ArrayList<>();
		for(int i=0;i<pagesCount_jqz;i++)
		{
			res_jqz.add(i+".db");
		}
		Collections.sort(res_jqz);
		String pagesTraceString_jqz = "jqz{ " + String.join(" ",res_jqz);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_jqz+")", fileManagerTrace2.contains(pagesTraceString_jqz));
		//--------------------------------------------------------------------------
		String[] cols3 = {"phx","r34","y7z","uu37","ecz2","kw","m78ps","f3s","wof","cz"};
		DBApp.createTable("ci7", cols3);
		for(int i=0;i<47;i++)
		{
			String [] record_ci7 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_ci7[j] = genRandString();
			}
			DBApp.insert("ci7", record_ci7);
		}
		int pagesCount_ci7 = (int)Math.ceil(47.0/DBApp.dataPageSize);
		ArrayList<String> res_ci7 = new ArrayList<>();
		for(int i=0;i<pagesCount_ci7;i++)
		{
			res_ci7.add(i+".db");
		}
		Collections.sort(res_ci7);
		String pagesTraceString_ci7 = "ci7{ " + String.join(" ",res_ci7);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ci7+")", fileManagerTrace3.contains(pagesTraceString_ci7));
		//--------------------------------------------------------------------------
		String[] cols4 = {"o63t","cdqm","x34ho","mpv","f8s"};
		DBApp.createTable("rq", cols4);
		for(int i=0;i<75;i++)
		{
			String [] record_rq = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_rq[j] = genRandString();
			}
			DBApp.insert("rq", record_rq);
		}
		int pagesCount_rq = (int)Math.ceil(75.0/DBApp.dataPageSize);
		ArrayList<String> res_rq = new ArrayList<>();
		for(int i=0;i<pagesCount_rq;i++)
		{
			res_rq.add(i+".db");
		}
		Collections.sort(res_rq);
		String pagesTraceString_rq = "rq{ " + String.join(" ",res_rq);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_rq+")", fileManagerTrace4.contains(pagesTraceString_rq));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test92TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"r43","xs1","z70","mk","xns"};
		DBApp.createTable("my", cols0);
		for(int i=0;i<64;i++)
		{
			String [] record_my = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_my[j] = genRandString();
			}
			DBApp.insert("my", record_my);
		}
		int pagesCount_my = (int)Math.ceil(64.0/DBApp.dataPageSize);
		ArrayList<String> res_my = new ArrayList<>();
		for(int i=0;i<pagesCount_my;i++)
		{
			res_my.add(i+".db");
		}
		Collections.sort(res_my);
		String pagesTraceString_my = "my{ " + String.join(" ",res_my);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_my+")", fileManagerTrace0.contains(pagesTraceString_my));
		//--------------------------------------------------------------------------
		String[] cols1 = {"dmgo4"};
		DBApp.createTable("pkj", cols1);
		for(int i=0;i<91;i++)
		{
			String [] record_pkj = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_pkj[j] = genRandString();
			}
			DBApp.insert("pkj", record_pkj);
		}
		int pagesCount_pkj = (int)Math.ceil(91.0/DBApp.dataPageSize);
		ArrayList<String> res_pkj = new ArrayList<>();
		for(int i=0;i<pagesCount_pkj;i++)
		{
			res_pkj.add(i+".db");
		}
		Collections.sort(res_pkj);
		String pagesTraceString_pkj = "pkj{ " + String.join(" ",res_pkj);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_pkj+")", fileManagerTrace1.contains(pagesTraceString_pkj));
		//--------------------------------------------------------------------------
		String[] cols2 = {"ru","tv","i10","g6","erk8","bu","xk34"};
		DBApp.createTable("ipg", cols2);
		for(int i=0;i<21;i++)
		{
			String [] record_ipg = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_ipg[j] = genRandString();
			}
			DBApp.insert("ipg", record_ipg);
		}
		int pagesCount_ipg = (int)Math.ceil(21.0/DBApp.dataPageSize);
		ArrayList<String> res_ipg = new ArrayList<>();
		for(int i=0;i<pagesCount_ipg;i++)
		{
			res_ipg.add(i+".db");
		}
		Collections.sort(res_ipg);
		String pagesTraceString_ipg = "ipg{ " + String.join(" ",res_ipg);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ipg+")", fileManagerTrace2.contains(pagesTraceString_ipg));
		//--------------------------------------------------------------------------
		String[] cols3 = {"i3f7v","ayt51"};
		DBApp.createTable("e388", cols3);
		for(int i=0;i<64;i++)
		{
			String [] record_e388 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_e388[j] = genRandString();
			}
			DBApp.insert("e388", record_e388);
		}
		int pagesCount_e388 = (int)Math.ceil(64.0/DBApp.dataPageSize);
		ArrayList<String> res_e388 = new ArrayList<>();
		for(int i=0;i<pagesCount_e388;i++)
		{
			res_e388.add(i+".db");
		}
		Collections.sort(res_e388);
		String pagesTraceString_e388 = "e388{ " + String.join(" ",res_e388);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_e388+")", fileManagerTrace3.contains(pagesTraceString_e388));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test93TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"y92","u1by","n57lb"};
		DBApp.createTable("dn3i", cols0);
		for(int i=0;i<13;i++)
		{
			String [] record_dn3i = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_dn3i[j] = genRandString();
			}
			DBApp.insert("dn3i", record_dn3i);
		}
		int pagesCount_dn3i = (int)Math.ceil(13.0/DBApp.dataPageSize);
		ArrayList<String> res_dn3i = new ArrayList<>();
		for(int i=0;i<pagesCount_dn3i;i++)
		{
			res_dn3i.add(i+".db");
		}
		Collections.sort(res_dn3i);
		String pagesTraceString_dn3i = "dn3i{ " + String.join(" ",res_dn3i);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_dn3i+")", fileManagerTrace0.contains(pagesTraceString_dn3i));
		//--------------------------------------------------------------------------
		String[] cols1 = {"l7945","j7i","w549l","rz7l","l4","t61","gl2","u4p","hn9"};
		DBApp.createTable("j403", cols1);
		for(int i=0;i<99;i++)
		{
			String [] record_j403 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_j403[j] = genRandString();
			}
			DBApp.insert("j403", record_j403);
		}
		int pagesCount_j403 = (int)Math.ceil(99.0/DBApp.dataPageSize);
		ArrayList<String> res_j403 = new ArrayList<>();
		for(int i=0;i<pagesCount_j403;i++)
		{
			res_j403.add(i+".db");
		}
		Collections.sort(res_j403);
		String pagesTraceString_j403 = "j403{ " + String.join(" ",res_j403);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_j403+")", fileManagerTrace1.contains(pagesTraceString_j403));
		//--------------------------------------------------------------------------
		String[] cols2 = {"n70h","yypq5"};
		DBApp.createTable("i9ih", cols2);
		for(int i=0;i<25;i++)
		{
			String [] record_i9ih = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_i9ih[j] = genRandString();
			}
			DBApp.insert("i9ih", record_i9ih);
		}
		int pagesCount_i9ih = (int)Math.ceil(25.0/DBApp.dataPageSize);
		ArrayList<String> res_i9ih = new ArrayList<>();
		for(int i=0;i<pagesCount_i9ih;i++)
		{
			res_i9ih.add(i+".db");
		}
		Collections.sort(res_i9ih);
		String pagesTraceString_i9ih = "i9ih{ " + String.join(" ",res_i9ih);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_i9ih+")", fileManagerTrace2.contains(pagesTraceString_i9ih));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test94TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"q2","lv4e0","fx8","e6","i2z","c7aj3","mbcq","g51","z3w","mzn22"};
		DBApp.createTable("z9e", cols0);
		for(int i=0;i<62;i++)
		{
			String [] record_z9e = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_z9e[j] = genRandString();
			}
			DBApp.insert("z9e", record_z9e);
		}
		int pagesCount_z9e = (int)Math.ceil(62.0/DBApp.dataPageSize);
		ArrayList<String> res_z9e = new ArrayList<>();
		for(int i=0;i<pagesCount_z9e;i++)
		{
			res_z9e.add(i+".db");
		}
		Collections.sort(res_z9e);
		String pagesTraceString_z9e = "z9e{ " + String.join(" ",res_z9e);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_z9e+")", fileManagerTrace0.contains(pagesTraceString_z9e));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test95TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"ut","s2","e9e","j53","dh7","k0","wbb","d48"};
		DBApp.createTable("pu", cols0);
		for(int i=0;i<4;i++)
		{
			String [] record_pu = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_pu[j] = genRandString();
			}
			DBApp.insert("pu", record_pu);
		}
		int pagesCount_pu = (int)Math.ceil(4.0/DBApp.dataPageSize);
		ArrayList<String> res_pu = new ArrayList<>();
		for(int i=0;i<pagesCount_pu;i++)
		{
			res_pu.add(i+".db");
		}
		Collections.sort(res_pu);
		String pagesTraceString_pu = "pu{ " + String.join(" ",res_pu);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_pu+")", fileManagerTrace0.contains(pagesTraceString_pu));
		//--------------------------------------------------------------------------
		String[] cols1 = {"u3u8","s88","kk"};
		DBApp.createTable("jsz", cols1);
		for(int i=0;i<52;i++)
		{
			String [] record_jsz = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_jsz[j] = genRandString();
			}
			DBApp.insert("jsz", record_jsz);
		}
		int pagesCount_jsz = (int)Math.ceil(52.0/DBApp.dataPageSize);
		ArrayList<String> res_jsz = new ArrayList<>();
		for(int i=0;i<pagesCount_jsz;i++)
		{
			res_jsz.add(i+".db");
		}
		Collections.sort(res_jsz);
		String pagesTraceString_jsz = "jsz{ " + String.join(" ",res_jsz);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_jsz+")", fileManagerTrace1.contains(pagesTraceString_jsz));
		//--------------------------------------------------------------------------
		String[] cols2 = {"qf","a59h","s5f"};
		DBApp.createTable("m6", cols2);
		for(int i=0;i<80;i++)
		{
			String [] record_m6 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_m6[j] = genRandString();
			}
			DBApp.insert("m6", record_m6);
		}
		int pagesCount_m6 = (int)Math.ceil(80.0/DBApp.dataPageSize);
		ArrayList<String> res_m6 = new ArrayList<>();
		for(int i=0;i<pagesCount_m6;i++)
		{
			res_m6.add(i+".db");
		}
		Collections.sort(res_m6);
		String pagesTraceString_m6 = "m6{ " + String.join(" ",res_m6);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_m6+")", fileManagerTrace2.contains(pagesTraceString_m6));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test96TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"x0","l44i2","h6yq9","s84x7","l6c","o67","qo9","vll1","x5h","a36"};
		DBApp.createTable("nyo", cols0);
		for(int i=0;i<97;i++)
		{
			String [] record_nyo = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_nyo[j] = genRandString();
			}
			DBApp.insert("nyo", record_nyo);
		}
		int pagesCount_nyo = (int)Math.ceil(97.0/DBApp.dataPageSize);
		ArrayList<String> res_nyo = new ArrayList<>();
		for(int i=0;i<pagesCount_nyo;i++)
		{
			res_nyo.add(i+".db");
		}
		Collections.sort(res_nyo);
		String pagesTraceString_nyo = "nyo{ " + String.join(" ",res_nyo);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_nyo+")", fileManagerTrace0.contains(pagesTraceString_nyo));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test97TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"n5u","k8ut","e9n89","xaq2b","wt465","n5"};
		DBApp.createTable("h6u", cols0);
		for(int i=0;i<91;i++)
		{
			String [] record_h6u = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_h6u[j] = genRandString();
			}
			DBApp.insert("h6u", record_h6u);
		}
		int pagesCount_h6u = (int)Math.ceil(91.0/DBApp.dataPageSize);
		ArrayList<String> res_h6u = new ArrayList<>();
		for(int i=0;i<pagesCount_h6u;i++)
		{
			res_h6u.add(i+".db");
		}
		Collections.sort(res_h6u);
		String pagesTraceString_h6u = "h6u{ " + String.join(" ",res_h6u);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_h6u+")", fileManagerTrace0.contains(pagesTraceString_h6u));
		//--------------------------------------------------------------------------
		String[] cols1 = {"lf","zgudo","i1","b5","z8","ix"};
		DBApp.createTable("y08", cols1);
		for(int i=0;i<100;i++)
		{
			String [] record_y08 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_y08[j] = genRandString();
			}
			DBApp.insert("y08", record_y08);
		}
		int pagesCount_y08 = (int)Math.ceil(100.0/DBApp.dataPageSize);
		ArrayList<String> res_y08 = new ArrayList<>();
		for(int i=0;i<pagesCount_y08;i++)
		{
			res_y08.add(i+".db");
		}
		Collections.sort(res_y08);
		String pagesTraceString_y08 = "y08{ " + String.join(" ",res_y08);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_y08+")", fileManagerTrace1.contains(pagesTraceString_y08));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test98TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"y5i"};
		DBApp.createTable("a78", cols0);
		for(int i=0;i<1;i++)
		{
			String [] record_a78 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_a78[j] = genRandString();
			}
			DBApp.insert("a78", record_a78);
		}
		int pagesCount_a78 = (int)Math.ceil(1.0/DBApp.dataPageSize);
		ArrayList<String> res_a78 = new ArrayList<>();
		for(int i=0;i<pagesCount_a78;i++)
		{
			res_a78.add(i+".db");
		}
		Collections.sort(res_a78);
		String pagesTraceString_a78 = "a78{ " + String.join(" ",res_a78);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_a78+")", fileManagerTrace0.contains(pagesTraceString_a78));
		//--------------------------------------------------------------------------
		String[] cols1 = {"fwrz","n784m","ii"};
		DBApp.createTable("ue0e4", cols1);
		for(int i=0;i<93;i++)
		{
			String [] record_ue0e4 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_ue0e4[j] = genRandString();
			}
			DBApp.insert("ue0e4", record_ue0e4);
		}
		int pagesCount_ue0e4 = (int)Math.ceil(93.0/DBApp.dataPageSize);
		ArrayList<String> res_ue0e4 = new ArrayList<>();
		for(int i=0;i<pagesCount_ue0e4;i++)
		{
			res_ue0e4.add(i+".db");
		}
		Collections.sort(res_ue0e4);
		String pagesTraceString_ue0e4 = "ue0e4{ " + String.join(" ",res_ue0e4);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ue0e4+")", fileManagerTrace1.contains(pagesTraceString_ue0e4));
		//--------------------------------------------------------------------------
		String[] cols2 = {"n71","opm"};
		DBApp.createTable("paff", cols2);
		for(int i=0;i<15;i++)
		{
			String [] record_paff = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_paff[j] = genRandString();
			}
			DBApp.insert("paff", record_paff);
		}
		int pagesCount_paff = (int)Math.ceil(15.0/DBApp.dataPageSize);
		ArrayList<String> res_paff = new ArrayList<>();
		for(int i=0;i<pagesCount_paff;i++)
		{
			res_paff.add(i+".db");
		}
		Collections.sort(res_paff);
		String pagesTraceString_paff = "paff{ " + String.join(" ",res_paff);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_paff+")", fileManagerTrace2.contains(pagesTraceString_paff));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test99TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"u3vk","ue","p0","k3l","no9","h0d2","rg0","g2m6","f52","f6"};
		DBApp.createTable("eg1", cols0);
		for(int i=0;i<36;i++)
		{
			String [] record_eg1 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_eg1[j] = genRandString();
			}
			DBApp.insert("eg1", record_eg1);
		}
		int pagesCount_eg1 = (int)Math.ceil(36.0/DBApp.dataPageSize);
		ArrayList<String> res_eg1 = new ArrayList<>();
		for(int i=0;i<pagesCount_eg1;i++)
		{
			res_eg1.add(i+".db");
		}
		Collections.sort(res_eg1);
		String pagesTraceString_eg1 = "eg1{ " + String.join(" ",res_eg1);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_eg1+")", fileManagerTrace0.contains(pagesTraceString_eg1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"h7z","t473","kno5","e1s3","p1","s39","j2pe","rj"};
		DBApp.createTable("t4gun", cols1);
		for(int i=0;i<17;i++)
		{
			String [] record_t4gun = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_t4gun[j] = genRandString();
			}
			DBApp.insert("t4gun", record_t4gun);
		}
		int pagesCount_t4gun = (int)Math.ceil(17.0/DBApp.dataPageSize);
		ArrayList<String> res_t4gun = new ArrayList<>();
		for(int i=0;i<pagesCount_t4gun;i++)
		{
			res_t4gun.add(i+".db");
		}
		Collections.sort(res_t4gun);
		String pagesTraceString_t4gun = "t4gun{ " + String.join(" ",res_t4gun);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_t4gun+")", fileManagerTrace1.contains(pagesTraceString_t4gun));
		//--------------------------------------------------------------------------
		String[] cols2 = {"z23","a0zk","rz2","bsg0b","ljc","zu4e6","ci7bz"};
		DBApp.createTable("dh", cols2);
		for(int i=0;i<78;i++)
		{
			String [] record_dh = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_dh[j] = genRandString();
			}
			DBApp.insert("dh", record_dh);
		}
		int pagesCount_dh = (int)Math.ceil(78.0/DBApp.dataPageSize);
		ArrayList<String> res_dh = new ArrayList<>();
		for(int i=0;i<pagesCount_dh;i++)
		{
			res_dh.add(i+".db");
		}
		Collections.sort(res_dh);
		String pagesTraceString_dh = "dh{ " + String.join(" ",res_dh);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_dh+")", fileManagerTrace2.contains(pagesTraceString_dh));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test100TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"wx","vqrq","pl4","w4x","wa4c","a1as"};
		DBApp.createTable("y0z2v", cols0);
		for(int i=0;i<80;i++)
		{
			String [] record_y0z2v = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_y0z2v[j] = genRandString();
			}
			DBApp.insert("y0z2v", record_y0z2v);
		}
		int pagesCount_y0z2v = (int)Math.ceil(80.0/DBApp.dataPageSize);
		ArrayList<String> res_y0z2v = new ArrayList<>();
		for(int i=0;i<pagesCount_y0z2v;i++)
		{
			res_y0z2v.add(i+".db");
		}
		Collections.sort(res_y0z2v);
		String pagesTraceString_y0z2v = "y0z2v{ " + String.join(" ",res_y0z2v);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_y0z2v+")", fileManagerTrace0.contains(pagesTraceString_y0z2v));
		//--------------------------------------------------------------------------
		String[] cols1 = {"xi4","olu1","sv9ev","o57","fbk","n9f","mtwg4","t081","zk6","i30"};
		DBApp.createTable("eep", cols1);
		for(int i=0;i<2;i++)
		{
			String [] record_eep = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_eep[j] = genRandString();
			}
			DBApp.insert("eep", record_eep);
		}
		int pagesCount_eep = (int)Math.ceil(2.0/DBApp.dataPageSize);
		ArrayList<String> res_eep = new ArrayList<>();
		for(int i=0;i<pagesCount_eep;i++)
		{
			res_eep.add(i+".db");
		}
		Collections.sort(res_eep);
		String pagesTraceString_eep = "eep{ " + String.join(" ",res_eep);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_eep+")", fileManagerTrace1.contains(pagesTraceString_eep));
		//--------------------------------------------------------------------------
		String[] cols2 = {"v3353","s4","r40","pj","vr","xti","v8i","mdz","p995l","lec"};
		DBApp.createTable("nk", cols2);
		for(int i=0;i<21;i++)
		{
			String [] record_nk = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_nk[j] = genRandString();
			}
			DBApp.insert("nk", record_nk);
		}
		int pagesCount_nk = (int)Math.ceil(21.0/DBApp.dataPageSize);
		ArrayList<String> res_nk = new ArrayList<>();
		for(int i=0;i<pagesCount_nk;i++)
		{
			res_nk.add(i+".db");
		}
		Collections.sort(res_nk);
		String pagesTraceString_nk = "nk{ " + String.join(" ",res_nk);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_nk+")", fileManagerTrace2.contains(pagesTraceString_nk));
		//--------------------------------------------------------------------------
		String[] cols3 = {"o579b","dm225","e4","dn","y8r7","zu5"};
		DBApp.createTable("xo2zl", cols3);
		for(int i=0;i<88;i++)
		{
			String [] record_xo2zl = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_xo2zl[j] = genRandString();
			}
			DBApp.insert("xo2zl", record_xo2zl);
		}
		int pagesCount_xo2zl = (int)Math.ceil(88.0/DBApp.dataPageSize);
		ArrayList<String> res_xo2zl = new ArrayList<>();
		for(int i=0;i<pagesCount_xo2zl;i++)
		{
			res_xo2zl.add(i+".db");
		}
		Collections.sort(res_xo2zl);
		String pagesTraceString_xo2zl = "xo2zl{ " + String.join(" ",res_xo2zl);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_xo2zl+")", fileManagerTrace3.contains(pagesTraceString_xo2zl));
		//--------------------------------------------------------------------------
		String[] cols4 = {"l4s"};
		DBApp.createTable("k80", cols4);
		for(int i=0;i<24;i++)
		{
			String [] record_k80 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_k80[j] = genRandString();
			}
			DBApp.insert("k80", record_k80);
		}
		int pagesCount_k80 = (int)Math.ceil(24.0/DBApp.dataPageSize);
		ArrayList<String> res_k80 = new ArrayList<>();
		for(int i=0;i<pagesCount_k80;i++)
		{
			res_k80.add(i+".db");
		}
		Collections.sort(res_k80);
		String pagesTraceString_k80 = "k80{ " + String.join(" ",res_k80);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_k80+")", fileManagerTrace4.contains(pagesTraceString_k80));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test101TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"iop","g47","f77i0","x1454","ny5","om920"};
		DBApp.createTable("wcewr", cols0);
		for(int i=0;i<84;i++)
		{
			String [] record_wcewr = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_wcewr[j] = genRandString();
			}
			DBApp.insert("wcewr", record_wcewr);
		}
		int pagesCount_wcewr = (int)Math.ceil(84.0/DBApp.dataPageSize);
		ArrayList<String> res_wcewr = new ArrayList<>();
		for(int i=0;i<pagesCount_wcewr;i++)
		{
			res_wcewr.add(i+".db");
		}
		Collections.sort(res_wcewr);
		String pagesTraceString_wcewr = "wcewr{ " + String.join(" ",res_wcewr);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_wcewr+")", fileManagerTrace0.contains(pagesTraceString_wcewr));
		//--------------------------------------------------------------------------
		String[] cols1 = {"c2","pp4","xg3","q0l","q4sj"};
		DBApp.createTable("n1", cols1);
		for(int i=0;i<48;i++)
		{
			String [] record_n1 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_n1[j] = genRandString();
			}
			DBApp.insert("n1", record_n1);
		}
		int pagesCount_n1 = (int)Math.ceil(48.0/DBApp.dataPageSize);
		ArrayList<String> res_n1 = new ArrayList<>();
		for(int i=0;i<pagesCount_n1;i++)
		{
			res_n1.add(i+".db");
		}
		Collections.sort(res_n1);
		String pagesTraceString_n1 = "n1{ " + String.join(" ",res_n1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_n1+")", fileManagerTrace1.contains(pagesTraceString_n1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"psy78","padis","jt5"};
		DBApp.createTable("v4", cols2);
		for(int i=0;i<51;i++)
		{
			String [] record_v4 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_v4[j] = genRandString();
			}
			DBApp.insert("v4", record_v4);
		}
		int pagesCount_v4 = (int)Math.ceil(51.0/DBApp.dataPageSize);
		ArrayList<String> res_v4 = new ArrayList<>();
		for(int i=0;i<pagesCount_v4;i++)
		{
			res_v4.add(i+".db");
		}
		Collections.sort(res_v4);
		String pagesTraceString_v4 = "v4{ " + String.join(" ",res_v4);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_v4+")", fileManagerTrace2.contains(pagesTraceString_v4));
		//--------------------------------------------------------------------------
		String[] cols3 = {"d3","or4","sbml","vu7","c9","h8u5","lu1"};
		DBApp.createTable("e4m", cols3);
		for(int i=0;i<89;i++)
		{
			String [] record_e4m = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_e4m[j] = genRandString();
			}
			DBApp.insert("e4m", record_e4m);
		}
		int pagesCount_e4m = (int)Math.ceil(89.0/DBApp.dataPageSize);
		ArrayList<String> res_e4m = new ArrayList<>();
		for(int i=0;i<pagesCount_e4m;i++)
		{
			res_e4m.add(i+".db");
		}
		Collections.sort(res_e4m);
		String pagesTraceString_e4m = "e4m{ " + String.join(" ",res_e4m);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_e4m+")", fileManagerTrace3.contains(pagesTraceString_e4m));
		//--------------------------------------------------------------------------
		String[] cols4 = {"ol","xr0","m28","p1","uf40"};
		DBApp.createTable("iii", cols4);
		for(int i=0;i<24;i++)
		{
			String [] record_iii = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_iii[j] = genRandString();
			}
			DBApp.insert("iii", record_iii);
		}
		int pagesCount_iii = (int)Math.ceil(24.0/DBApp.dataPageSize);
		ArrayList<String> res_iii = new ArrayList<>();
		for(int i=0;i<pagesCount_iii;i++)
		{
			res_iii.add(i+".db");
		}
		Collections.sort(res_iii);
		String pagesTraceString_iii = "iii{ " + String.join(" ",res_iii);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_iii+")", fileManagerTrace4.contains(pagesTraceString_iii));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test102TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"fr8","dw3","d3hl","r0c","et","rukq4","a92i","i7","o19v4","gqa6"};
		DBApp.createTable("z0e2", cols0);
		for(int i=0;i<93;i++)
		{
			String [] record_z0e2 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_z0e2[j] = genRandString();
			}
			DBApp.insert("z0e2", record_z0e2);
		}
		int pagesCount_z0e2 = (int)Math.ceil(93.0/DBApp.dataPageSize);
		ArrayList<String> res_z0e2 = new ArrayList<>();
		for(int i=0;i<pagesCount_z0e2;i++)
		{
			res_z0e2.add(i+".db");
		}
		Collections.sort(res_z0e2);
		String pagesTraceString_z0e2 = "z0e2{ " + String.join(" ",res_z0e2);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_z0e2+")", fileManagerTrace0.contains(pagesTraceString_z0e2));
		//--------------------------------------------------------------------------
		String[] cols1 = {"xg9","c8cf1","zjb","kt0","chzq6"};
		DBApp.createTable("b4r", cols1);
		for(int i=0;i<80;i++)
		{
			String [] record_b4r = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_b4r[j] = genRandString();
			}
			DBApp.insert("b4r", record_b4r);
		}
		int pagesCount_b4r = (int)Math.ceil(80.0/DBApp.dataPageSize);
		ArrayList<String> res_b4r = new ArrayList<>();
		for(int i=0;i<pagesCount_b4r;i++)
		{
			res_b4r.add(i+".db");
		}
		Collections.sort(res_b4r);
		String pagesTraceString_b4r = "b4r{ " + String.join(" ",res_b4r);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_b4r+")", fileManagerTrace1.contains(pagesTraceString_b4r));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test103TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"n8j6j","g48","qq1","s5u","vj39","o1va","u8s0","yk2id","rd6","m98d"};
		DBApp.createTable("yxsr7", cols0);
		for(int i=0;i<6;i++)
		{
			String [] record_yxsr7 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_yxsr7[j] = genRandString();
			}
			DBApp.insert("yxsr7", record_yxsr7);
		}
		int pagesCount_yxsr7 = (int)Math.ceil(6.0/DBApp.dataPageSize);
		ArrayList<String> res_yxsr7 = new ArrayList<>();
		for(int i=0;i<pagesCount_yxsr7;i++)
		{
			res_yxsr7.add(i+".db");
		}
		Collections.sort(res_yxsr7);
		String pagesTraceString_yxsr7 = "yxsr7{ " + String.join(" ",res_yxsr7);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_yxsr7+")", fileManagerTrace0.contains(pagesTraceString_yxsr7));
		//--------------------------------------------------------------------------
		String[] cols1 = {"sxc","yq2g1","np67","nmu","dz"};
		DBApp.createTable("k2", cols1);
		for(int i=0;i<63;i++)
		{
			String [] record_k2 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_k2[j] = genRandString();
			}
			DBApp.insert("k2", record_k2);
		}
		int pagesCount_k2 = (int)Math.ceil(63.0/DBApp.dataPageSize);
		ArrayList<String> res_k2 = new ArrayList<>();
		for(int i=0;i<pagesCount_k2;i++)
		{
			res_k2.add(i+".db");
		}
		Collections.sort(res_k2);
		String pagesTraceString_k2 = "k2{ " + String.join(" ",res_k2);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_k2+")", fileManagerTrace1.contains(pagesTraceString_k2));
		//--------------------------------------------------------------------------
		String[] cols2 = {"tuz1","j8y","k13","y2d","au9","mm7u"};
		DBApp.createTable("qw", cols2);
		for(int i=0;i<100;i++)
		{
			String [] record_qw = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_qw[j] = genRandString();
			}
			DBApp.insert("qw", record_qw);
		}
		int pagesCount_qw = (int)Math.ceil(100.0/DBApp.dataPageSize);
		ArrayList<String> res_qw = new ArrayList<>();
		for(int i=0;i<pagesCount_qw;i++)
		{
			res_qw.add(i+".db");
		}
		Collections.sort(res_qw);
		String pagesTraceString_qw = "qw{ " + String.join(" ",res_qw);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_qw+")", fileManagerTrace2.contains(pagesTraceString_qw));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test104TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"o1j"};
		DBApp.createTable("v1g", cols0);
		for(int i=0;i<15;i++)
		{
			String [] record_v1g = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_v1g[j] = genRandString();
			}
			DBApp.insert("v1g", record_v1g);
		}
		int pagesCount_v1g = (int)Math.ceil(15.0/DBApp.dataPageSize);
		ArrayList<String> res_v1g = new ArrayList<>();
		for(int i=0;i<pagesCount_v1g;i++)
		{
			res_v1g.add(i+".db");
		}
		Collections.sort(res_v1g);
		String pagesTraceString_v1g = "v1g{ " + String.join(" ",res_v1g);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_v1g+")", fileManagerTrace0.contains(pagesTraceString_v1g));
		//--------------------------------------------------------------------------
		String[] cols1 = {"w351","a7j","lyu","mr7","hr354","leu86"};
		DBApp.createTable("xy", cols1);
		for(int i=0;i<18;i++)
		{
			String [] record_xy = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_xy[j] = genRandString();
			}
			DBApp.insert("xy", record_xy);
		}
		int pagesCount_xy = (int)Math.ceil(18.0/DBApp.dataPageSize);
		ArrayList<String> res_xy = new ArrayList<>();
		for(int i=0;i<pagesCount_xy;i++)
		{
			res_xy.add(i+".db");
		}
		Collections.sort(res_xy);
		String pagesTraceString_xy = "xy{ " + String.join(" ",res_xy);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_xy+")", fileManagerTrace1.contains(pagesTraceString_xy));
		//--------------------------------------------------------------------------
		String[] cols2 = {"vko","tw7p6","fc9","ru"};
		DBApp.createTable("e55", cols2);
		for(int i=0;i<44;i++)
		{
			String [] record_e55 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_e55[j] = genRandString();
			}
			DBApp.insert("e55", record_e55);
		}
		int pagesCount_e55 = (int)Math.ceil(44.0/DBApp.dataPageSize);
		ArrayList<String> res_e55 = new ArrayList<>();
		for(int i=0;i<pagesCount_e55;i++)
		{
			res_e55.add(i+".db");
		}
		Collections.sort(res_e55);
		String pagesTraceString_e55 = "e55{ " + String.join(" ",res_e55);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_e55+")", fileManagerTrace2.contains(pagesTraceString_e55));
		//--------------------------------------------------------------------------
		String[] cols3 = {"o81","wb","qw1","ivf3e","h1z28","v0","p13","dat"};
		DBApp.createTable("y54", cols3);
		for(int i=0;i<14;i++)
		{
			String [] record_y54 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_y54[j] = genRandString();
			}
			DBApp.insert("y54", record_y54);
		}
		int pagesCount_y54 = (int)Math.ceil(14.0/DBApp.dataPageSize);
		ArrayList<String> res_y54 = new ArrayList<>();
		for(int i=0;i<pagesCount_y54;i++)
		{
			res_y54.add(i+".db");
		}
		Collections.sort(res_y54);
		String pagesTraceString_y54 = "y54{ " + String.join(" ",res_y54);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_y54+")", fileManagerTrace3.contains(pagesTraceString_y54));
		//--------------------------------------------------------------------------
		String[] cols4 = {"ie5","n5","z2"};
		DBApp.createTable("t3i", cols4);
		for(int i=0;i<99;i++)
		{
			String [] record_t3i = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_t3i[j] = genRandString();
			}
			DBApp.insert("t3i", record_t3i);
		}
		int pagesCount_t3i = (int)Math.ceil(99.0/DBApp.dataPageSize);
		ArrayList<String> res_t3i = new ArrayList<>();
		for(int i=0;i<pagesCount_t3i;i++)
		{
			res_t3i.add(i+".db");
		}
		Collections.sort(res_t3i);
		String pagesTraceString_t3i = "t3i{ " + String.join(" ",res_t3i);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_t3i+")", fileManagerTrace4.contains(pagesTraceString_t3i));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test105TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"azll"};
		DBApp.createTable("afx", cols0);
		for(int i=0;i<64;i++)
		{
			String [] record_afx = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_afx[j] = genRandString();
			}
			DBApp.insert("afx", record_afx);
		}
		int pagesCount_afx = (int)Math.ceil(64.0/DBApp.dataPageSize);
		ArrayList<String> res_afx = new ArrayList<>();
		for(int i=0;i<pagesCount_afx;i++)
		{
			res_afx.add(i+".db");
		}
		Collections.sort(res_afx);
		String pagesTraceString_afx = "afx{ " + String.join(" ",res_afx);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_afx+")", fileManagerTrace0.contains(pagesTraceString_afx));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test106TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"qtb","hgd","h9u","m7vj"};
		DBApp.createTable("ku", cols0);
		for(int i=0;i<93;i++)
		{
			String [] record_ku = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ku[j] = genRandString();
			}
			DBApp.insert("ku", record_ku);
		}
		int pagesCount_ku = (int)Math.ceil(93.0/DBApp.dataPageSize);
		ArrayList<String> res_ku = new ArrayList<>();
		for(int i=0;i<pagesCount_ku;i++)
		{
			res_ku.add(i+".db");
		}
		Collections.sort(res_ku);
		String pagesTraceString_ku = "ku{ " + String.join(" ",res_ku);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ku+")", fileManagerTrace0.contains(pagesTraceString_ku));
		//--------------------------------------------------------------------------
		String[] cols1 = {"nz71h","dm3w","k704n","d17","it","mr99h"};
		DBApp.createTable("qt1", cols1);
		for(int i=0;i<80;i++)
		{
			String [] record_qt1 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_qt1[j] = genRandString();
			}
			DBApp.insert("qt1", record_qt1);
		}
		int pagesCount_qt1 = (int)Math.ceil(80.0/DBApp.dataPageSize);
		ArrayList<String> res_qt1 = new ArrayList<>();
		for(int i=0;i<pagesCount_qt1;i++)
		{
			res_qt1.add(i+".db");
		}
		Collections.sort(res_qt1);
		String pagesTraceString_qt1 = "qt1{ " + String.join(" ",res_qt1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_qt1+")", fileManagerTrace1.contains(pagesTraceString_qt1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"v5","dff","w7","n1","tq7","w05"};
		DBApp.createTable("n85", cols2);
		for(int i=0;i<75;i++)
		{
			String [] record_n85 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_n85[j] = genRandString();
			}
			DBApp.insert("n85", record_n85);
		}
		int pagesCount_n85 = (int)Math.ceil(75.0/DBApp.dataPageSize);
		ArrayList<String> res_n85 = new ArrayList<>();
		for(int i=0;i<pagesCount_n85;i++)
		{
			res_n85.add(i+".db");
		}
		Collections.sort(res_n85);
		String pagesTraceString_n85 = "n85{ " + String.join(" ",res_n85);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_n85+")", fileManagerTrace2.contains(pagesTraceString_n85));
		//--------------------------------------------------------------------------
		String[] cols3 = {"cyq"};
		DBApp.createTable("qj6", cols3);
		for(int i=0;i<32;i++)
		{
			String [] record_qj6 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_qj6[j] = genRandString();
			}
			DBApp.insert("qj6", record_qj6);
		}
		int pagesCount_qj6 = (int)Math.ceil(32.0/DBApp.dataPageSize);
		ArrayList<String> res_qj6 = new ArrayList<>();
		for(int i=0;i<pagesCount_qj6;i++)
		{
			res_qj6.add(i+".db");
		}
		Collections.sort(res_qj6);
		String pagesTraceString_qj6 = "qj6{ " + String.join(" ",res_qj6);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_qj6+")", fileManagerTrace3.contains(pagesTraceString_qj6));
		//--------------------------------------------------------------------------
		String[] cols4 = {"h6m","fau","a2","qls","b46x","dq","nkxpb","nv"};
		DBApp.createTable("fxc", cols4);
		for(int i=0;i<45;i++)
		{
			String [] record_fxc = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_fxc[j] = genRandString();
			}
			DBApp.insert("fxc", record_fxc);
		}
		int pagesCount_fxc = (int)Math.ceil(45.0/DBApp.dataPageSize);
		ArrayList<String> res_fxc = new ArrayList<>();
		for(int i=0;i<pagesCount_fxc;i++)
		{
			res_fxc.add(i+".db");
		}
		Collections.sort(res_fxc);
		String pagesTraceString_fxc = "fxc{ " + String.join(" ",res_fxc);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_fxc+")", fileManagerTrace4.contains(pagesTraceString_fxc));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test107TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"xm4g","v9tv6","dic7","qlo38","u9g","t89","cw4z","pjml7","o0th"};
		DBApp.createTable("a0r", cols0);
		for(int i=0;i<48;i++)
		{
			String [] record_a0r = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_a0r[j] = genRandString();
			}
			DBApp.insert("a0r", record_a0r);
		}
		int pagesCount_a0r = (int)Math.ceil(48.0/DBApp.dataPageSize);
		ArrayList<String> res_a0r = new ArrayList<>();
		for(int i=0;i<pagesCount_a0r;i++)
		{
			res_a0r.add(i+".db");
		}
		Collections.sort(res_a0r);
		String pagesTraceString_a0r = "a0r{ " + String.join(" ",res_a0r);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_a0r+")", fileManagerTrace0.contains(pagesTraceString_a0r));
		//--------------------------------------------------------------------------
		String[] cols1 = {"t34","rl62k","n07","ve232","z9","nkm5","m5a6","u07","o1e","tfo"};
		DBApp.createTable("a68", cols1);
		for(int i=0;i<93;i++)
		{
			String [] record_a68 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_a68[j] = genRandString();
			}
			DBApp.insert("a68", record_a68);
		}
		int pagesCount_a68 = (int)Math.ceil(93.0/DBApp.dataPageSize);
		ArrayList<String> res_a68 = new ArrayList<>();
		for(int i=0;i<pagesCount_a68;i++)
		{
			res_a68.add(i+".db");
		}
		Collections.sort(res_a68);
		String pagesTraceString_a68 = "a68{ " + String.join(" ",res_a68);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_a68+")", fileManagerTrace1.contains(pagesTraceString_a68));
		//--------------------------------------------------------------------------
		String[] cols2 = {"tf1"};
		DBApp.createTable("fs2", cols2);
		for(int i=0;i<92;i++)
		{
			String [] record_fs2 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_fs2[j] = genRandString();
			}
			DBApp.insert("fs2", record_fs2);
		}
		int pagesCount_fs2 = (int)Math.ceil(92.0/DBApp.dataPageSize);
		ArrayList<String> res_fs2 = new ArrayList<>();
		for(int i=0;i<pagesCount_fs2;i++)
		{
			res_fs2.add(i+".db");
		}
		Collections.sort(res_fs2);
		String pagesTraceString_fs2 = "fs2{ " + String.join(" ",res_fs2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_fs2+")", fileManagerTrace2.contains(pagesTraceString_fs2));
		//--------------------------------------------------------------------------
		String[] cols3 = {"neev","rki"};
		DBApp.createTable("kwf", cols3);
		for(int i=0;i<39;i++)
		{
			String [] record_kwf = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_kwf[j] = genRandString();
			}
			DBApp.insert("kwf", record_kwf);
		}
		int pagesCount_kwf = (int)Math.ceil(39.0/DBApp.dataPageSize);
		ArrayList<String> res_kwf = new ArrayList<>();
		for(int i=0;i<pagesCount_kwf;i++)
		{
			res_kwf.add(i+".db");
		}
		Collections.sort(res_kwf);
		String pagesTraceString_kwf = "kwf{ " + String.join(" ",res_kwf);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_kwf+")", fileManagerTrace3.contains(pagesTraceString_kwf));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test108TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"dx0","d338","eg5","w17","d4","l5p","d39","p2i"};
		DBApp.createTable("lky", cols0);
		for(int i=0;i<93;i++)
		{
			String [] record_lky = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_lky[j] = genRandString();
			}
			DBApp.insert("lky", record_lky);
		}
		int pagesCount_lky = (int)Math.ceil(93.0/DBApp.dataPageSize);
		ArrayList<String> res_lky = new ArrayList<>();
		for(int i=0;i<pagesCount_lky;i++)
		{
			res_lky.add(i+".db");
		}
		Collections.sort(res_lky);
		String pagesTraceString_lky = "lky{ " + String.join(" ",res_lky);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_lky+")", fileManagerTrace0.contains(pagesTraceString_lky));
		//--------------------------------------------------------------------------
		String[] cols1 = {"mx8cg","bu50","spx","v2","pgmi","i27vc","o0440","fp"};
		DBApp.createTable("cw2", cols1);
		for(int i=0;i<75;i++)
		{
			String [] record_cw2 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_cw2[j] = genRandString();
			}
			DBApp.insert("cw2", record_cw2);
		}
		int pagesCount_cw2 = (int)Math.ceil(75.0/DBApp.dataPageSize);
		ArrayList<String> res_cw2 = new ArrayList<>();
		for(int i=0;i<pagesCount_cw2;i++)
		{
			res_cw2.add(i+".db");
		}
		Collections.sort(res_cw2);
		String pagesTraceString_cw2 = "cw2{ " + String.join(" ",res_cw2);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_cw2+")", fileManagerTrace1.contains(pagesTraceString_cw2));
		//--------------------------------------------------------------------------
		String[] cols2 = {"qxo","g0h","kmngm","p50","n1u","xu2e","vz14n"};
		DBApp.createTable("x8mse", cols2);
		for(int i=0;i<40;i++)
		{
			String [] record_x8mse = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_x8mse[j] = genRandString();
			}
			DBApp.insert("x8mse", record_x8mse);
		}
		int pagesCount_x8mse = (int)Math.ceil(40.0/DBApp.dataPageSize);
		ArrayList<String> res_x8mse = new ArrayList<>();
		for(int i=0;i<pagesCount_x8mse;i++)
		{
			res_x8mse.add(i+".db");
		}
		Collections.sort(res_x8mse);
		String pagesTraceString_x8mse = "x8mse{ " + String.join(" ",res_x8mse);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_x8mse+")", fileManagerTrace2.contains(pagesTraceString_x8mse));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test109TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"z9z","var","w1d","d0osh","n48","hnu","i76b8","bhs37","x7738","q38k"};
		DBApp.createTable("r4cvy", cols0);
		for(int i=0;i<31;i++)
		{
			String [] record_r4cvy = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_r4cvy[j] = genRandString();
			}
			DBApp.insert("r4cvy", record_r4cvy);
		}
		int pagesCount_r4cvy = (int)Math.ceil(31.0/DBApp.dataPageSize);
		ArrayList<String> res_r4cvy = new ArrayList<>();
		for(int i=0;i<pagesCount_r4cvy;i++)
		{
			res_r4cvy.add(i+".db");
		}
		Collections.sort(res_r4cvy);
		String pagesTraceString_r4cvy = "r4cvy{ " + String.join(" ",res_r4cvy);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_r4cvy+")", fileManagerTrace0.contains(pagesTraceString_r4cvy));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test110TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"d8k","f53"};
		DBApp.createTable("hzz", cols0);
		for(int i=0;i<57;i++)
		{
			String [] record_hzz = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_hzz[j] = genRandString();
			}
			DBApp.insert("hzz", record_hzz);
		}
		int pagesCount_hzz = (int)Math.ceil(57.0/DBApp.dataPageSize);
		ArrayList<String> res_hzz = new ArrayList<>();
		for(int i=0;i<pagesCount_hzz;i++)
		{
			res_hzz.add(i+".db");
		}
		Collections.sort(res_hzz);
		String pagesTraceString_hzz = "hzz{ " + String.join(" ",res_hzz);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_hzz+")", fileManagerTrace0.contains(pagesTraceString_hzz));
		//--------------------------------------------------------------------------
		String[] cols1 = {"anm","f79e","a735u","g68","eg6bg","t23","yg","s5","cyz"};
		DBApp.createTable("sanp", cols1);
		for(int i=0;i<98;i++)
		{
			String [] record_sanp = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_sanp[j] = genRandString();
			}
			DBApp.insert("sanp", record_sanp);
		}
		int pagesCount_sanp = (int)Math.ceil(98.0/DBApp.dataPageSize);
		ArrayList<String> res_sanp = new ArrayList<>();
		for(int i=0;i<pagesCount_sanp;i++)
		{
			res_sanp.add(i+".db");
		}
		Collections.sort(res_sanp);
		String pagesTraceString_sanp = "sanp{ " + String.join(" ",res_sanp);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_sanp+")", fileManagerTrace1.contains(pagesTraceString_sanp));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test111TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"ybo","v70","dzw3b","qn","iz3","ihe3"};
		DBApp.createTable("dp", cols0);
		for(int i=0;i<57;i++)
		{
			String [] record_dp = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_dp[j] = genRandString();
			}
			DBApp.insert("dp", record_dp);
		}
		int pagesCount_dp = (int)Math.ceil(57.0/DBApp.dataPageSize);
		ArrayList<String> res_dp = new ArrayList<>();
		for(int i=0;i<pagesCount_dp;i++)
		{
			res_dp.add(i+".db");
		}
		Collections.sort(res_dp);
		String pagesTraceString_dp = "dp{ " + String.join(" ",res_dp);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_dp+")", fileManagerTrace0.contains(pagesTraceString_dp));
		//--------------------------------------------------------------------------
		String[] cols1 = {"vz6","b772","xpc","s1","ncn","c3q"};
		DBApp.createTable("c4u", cols1);
		for(int i=0;i<10;i++)
		{
			String [] record_c4u = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_c4u[j] = genRandString();
			}
			DBApp.insert("c4u", record_c4u);
		}
		int pagesCount_c4u = (int)Math.ceil(10.0/DBApp.dataPageSize);
		ArrayList<String> res_c4u = new ArrayList<>();
		for(int i=0;i<pagesCount_c4u;i++)
		{
			res_c4u.add(i+".db");
		}
		Collections.sort(res_c4u);
		String pagesTraceString_c4u = "c4u{ " + String.join(" ",res_c4u);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_c4u+")", fileManagerTrace1.contains(pagesTraceString_c4u));
		//--------------------------------------------------------------------------
		String[] cols2 = {"cbrn","kol","nkm"};
		DBApp.createTable("jp7", cols2);
		for(int i=0;i<31;i++)
		{
			String [] record_jp7 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_jp7[j] = genRandString();
			}
			DBApp.insert("jp7", record_jp7);
		}
		int pagesCount_jp7 = (int)Math.ceil(31.0/DBApp.dataPageSize);
		ArrayList<String> res_jp7 = new ArrayList<>();
		for(int i=0;i<pagesCount_jp7;i++)
		{
			res_jp7.add(i+".db");
		}
		Collections.sort(res_jp7);
		String pagesTraceString_jp7 = "jp7{ " + String.join(" ",res_jp7);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_jp7+")", fileManagerTrace2.contains(pagesTraceString_jp7));
		//--------------------------------------------------------------------------
		String[] cols3 = {"n00","h0","anbe"};
		DBApp.createTable("b95", cols3);
		for(int i=0;i<20;i++)
		{
			String [] record_b95 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_b95[j] = genRandString();
			}
			DBApp.insert("b95", record_b95);
		}
		int pagesCount_b95 = (int)Math.ceil(20.0/DBApp.dataPageSize);
		ArrayList<String> res_b95 = new ArrayList<>();
		for(int i=0;i<pagesCount_b95;i++)
		{
			res_b95.add(i+".db");
		}
		Collections.sort(res_b95);
		String pagesTraceString_b95 = "b95{ " + String.join(" ",res_b95);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_b95+")", fileManagerTrace3.contains(pagesTraceString_b95));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test112TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"fyz","q18q","hpg32","q5e5","r3qs","swyv3","xw8"};
		DBApp.createTable("r20", cols0);
		for(int i=0;i<31;i++)
		{
			String [] record_r20 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_r20[j] = genRandString();
			}
			DBApp.insert("r20", record_r20);
		}
		int pagesCount_r20 = (int)Math.ceil(31.0/DBApp.dataPageSize);
		ArrayList<String> res_r20 = new ArrayList<>();
		for(int i=0;i<pagesCount_r20;i++)
		{
			res_r20.add(i+".db");
		}
		Collections.sort(res_r20);
		String pagesTraceString_r20 = "r20{ " + String.join(" ",res_r20);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_r20+")", fileManagerTrace0.contains(pagesTraceString_r20));
		//--------------------------------------------------------------------------
		String[] cols1 = {"nc40","x27","w9r"};
		DBApp.createTable("v01mg", cols1);
		for(int i=0;i<55;i++)
		{
			String [] record_v01mg = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_v01mg[j] = genRandString();
			}
			DBApp.insert("v01mg", record_v01mg);
		}
		int pagesCount_v01mg = (int)Math.ceil(55.0/DBApp.dataPageSize);
		ArrayList<String> res_v01mg = new ArrayList<>();
		for(int i=0;i<pagesCount_v01mg;i++)
		{
			res_v01mg.add(i+".db");
		}
		Collections.sort(res_v01mg);
		String pagesTraceString_v01mg = "v01mg{ " + String.join(" ",res_v01mg);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_v01mg+")", fileManagerTrace1.contains(pagesTraceString_v01mg));
		//--------------------------------------------------------------------------
		String[] cols2 = {"kc68","m33","xouv","igs73"};
		DBApp.createTable("zxr", cols2);
		for(int i=0;i<38;i++)
		{
			String [] record_zxr = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_zxr[j] = genRandString();
			}
			DBApp.insert("zxr", record_zxr);
		}
		int pagesCount_zxr = (int)Math.ceil(38.0/DBApp.dataPageSize);
		ArrayList<String> res_zxr = new ArrayList<>();
		for(int i=0;i<pagesCount_zxr;i++)
		{
			res_zxr.add(i+".db");
		}
		Collections.sort(res_zxr);
		String pagesTraceString_zxr = "zxr{ " + String.join(" ",res_zxr);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_zxr+")", fileManagerTrace2.contains(pagesTraceString_zxr));
		//--------------------------------------------------------------------------
		String[] cols3 = {"n22n","u9i6","rn6j0","y068j","a731","j95","z8d","w4z43"};
		DBApp.createTable("w8h5a", cols3);
		for(int i=0;i<62;i++)
		{
			String [] record_w8h5a = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_w8h5a[j] = genRandString();
			}
			DBApp.insert("w8h5a", record_w8h5a);
		}
		int pagesCount_w8h5a = (int)Math.ceil(62.0/DBApp.dataPageSize);
		ArrayList<String> res_w8h5a = new ArrayList<>();
		for(int i=0;i<pagesCount_w8h5a;i++)
		{
			res_w8h5a.add(i+".db");
		}
		Collections.sort(res_w8h5a);
		String pagesTraceString_w8h5a = "w8h5a{ " + String.join(" ",res_w8h5a);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_w8h5a+")", fileManagerTrace3.contains(pagesTraceString_w8h5a));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test113TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"f0hbr"};
		DBApp.createTable("q9h", cols0);
		for(int i=0;i<50;i++)
		{
			String [] record_q9h = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_q9h[j] = genRandString();
			}
			DBApp.insert("q9h", record_q9h);
		}
		int pagesCount_q9h = (int)Math.ceil(50.0/DBApp.dataPageSize);
		ArrayList<String> res_q9h = new ArrayList<>();
		for(int i=0;i<pagesCount_q9h;i++)
		{
			res_q9h.add(i+".db");
		}
		Collections.sort(res_q9h);
		String pagesTraceString_q9h = "q9h{ " + String.join(" ",res_q9h);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_q9h+")", fileManagerTrace0.contains(pagesTraceString_q9h));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test114TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"f7x","r2j"};
		DBApp.createTable("m78k9", cols0);
		for(int i=0;i<55;i++)
		{
			String [] record_m78k9 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_m78k9[j] = genRandString();
			}
			DBApp.insert("m78k9", record_m78k9);
		}
		int pagesCount_m78k9 = (int)Math.ceil(55.0/DBApp.dataPageSize);
		ArrayList<String> res_m78k9 = new ArrayList<>();
		for(int i=0;i<pagesCount_m78k9;i++)
		{
			res_m78k9.add(i+".db");
		}
		Collections.sort(res_m78k9);
		String pagesTraceString_m78k9 = "m78k9{ " + String.join(" ",res_m78k9);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_m78k9+")", fileManagerTrace0.contains(pagesTraceString_m78k9));
		//--------------------------------------------------------------------------
		String[] cols1 = {"urh"};
		DBApp.createTable("oi1", cols1);
		for(int i=0;i<56;i++)
		{
			String [] record_oi1 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_oi1[j] = genRandString();
			}
			DBApp.insert("oi1", record_oi1);
		}
		int pagesCount_oi1 = (int)Math.ceil(56.0/DBApp.dataPageSize);
		ArrayList<String> res_oi1 = new ArrayList<>();
		for(int i=0;i<pagesCount_oi1;i++)
		{
			res_oi1.add(i+".db");
		}
		Collections.sort(res_oi1);
		String pagesTraceString_oi1 = "oi1{ " + String.join(" ",res_oi1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_oi1+")", fileManagerTrace1.contains(pagesTraceString_oi1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"ey01","r94","k7","ag0o","y61"};
		DBApp.createTable("h369i", cols2);
		for(int i=0;i<30;i++)
		{
			String [] record_h369i = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_h369i[j] = genRandString();
			}
			DBApp.insert("h369i", record_h369i);
		}
		int pagesCount_h369i = (int)Math.ceil(30.0/DBApp.dataPageSize);
		ArrayList<String> res_h369i = new ArrayList<>();
		for(int i=0;i<pagesCount_h369i;i++)
		{
			res_h369i.add(i+".db");
		}
		Collections.sort(res_h369i);
		String pagesTraceString_h369i = "h369i{ " + String.join(" ",res_h369i);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_h369i+")", fileManagerTrace2.contains(pagesTraceString_h369i));
		//--------------------------------------------------------------------------
		String[] cols3 = {"y0","wylu","poj","z01x4","zzy","far4"};
		DBApp.createTable("gf", cols3);
		for(int i=0;i<61;i++)
		{
			String [] record_gf = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_gf[j] = genRandString();
			}
			DBApp.insert("gf", record_gf);
		}
		int pagesCount_gf = (int)Math.ceil(61.0/DBApp.dataPageSize);
		ArrayList<String> res_gf = new ArrayList<>();
		for(int i=0;i<pagesCount_gf;i++)
		{
			res_gf.add(i+".db");
		}
		Collections.sort(res_gf);
		String pagesTraceString_gf = "gf{ " + String.join(" ",res_gf);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_gf+")", fileManagerTrace3.contains(pagesTraceString_gf));
		//--------------------------------------------------------------------------
		String[] cols4 = {"yq","ro4","j7ioa","sok","gpr","voa","qc64","nb"};
		DBApp.createTable("obc", cols4);
		for(int i=0;i<59;i++)
		{
			String [] record_obc = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_obc[j] = genRandString();
			}
			DBApp.insert("obc", record_obc);
		}
		int pagesCount_obc = (int)Math.ceil(59.0/DBApp.dataPageSize);
		ArrayList<String> res_obc = new ArrayList<>();
		for(int i=0;i<pagesCount_obc;i++)
		{
			res_obc.add(i+".db");
		}
		Collections.sort(res_obc);
		String pagesTraceString_obc = "obc{ " + String.join(" ",res_obc);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_obc+")", fileManagerTrace4.contains(pagesTraceString_obc));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test115TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"cgg","bp9","iy16","x1pw1","n4hz3","i9rei","pucm","h5qr"};
		DBApp.createTable("mrea", cols0);
		for(int i=0;i<13;i++)
		{
			String [] record_mrea = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_mrea[j] = genRandString();
			}
			DBApp.insert("mrea", record_mrea);
		}
		int pagesCount_mrea = (int)Math.ceil(13.0/DBApp.dataPageSize);
		ArrayList<String> res_mrea = new ArrayList<>();
		for(int i=0;i<pagesCount_mrea;i++)
		{
			res_mrea.add(i+".db");
		}
		Collections.sort(res_mrea);
		String pagesTraceString_mrea = "mrea{ " + String.join(" ",res_mrea);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_mrea+")", fileManagerTrace0.contains(pagesTraceString_mrea));
		//--------------------------------------------------------------------------
		String[] cols1 = {"vt","bo"};
		DBApp.createTable("k7", cols1);
		for(int i=0;i<45;i++)
		{
			String [] record_k7 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_k7[j] = genRandString();
			}
			DBApp.insert("k7", record_k7);
		}
		int pagesCount_k7 = (int)Math.ceil(45.0/DBApp.dataPageSize);
		ArrayList<String> res_k7 = new ArrayList<>();
		for(int i=0;i<pagesCount_k7;i++)
		{
			res_k7.add(i+".db");
		}
		Collections.sort(res_k7);
		String pagesTraceString_k7 = "k7{ " + String.join(" ",res_k7);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_k7+")", fileManagerTrace1.contains(pagesTraceString_k7));
		//--------------------------------------------------------------------------
		String[] cols2 = {"kb3","pmj","hv3","k0p"};
		DBApp.createTable("xzft", cols2);
		for(int i=0;i<29;i++)
		{
			String [] record_xzft = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_xzft[j] = genRandString();
			}
			DBApp.insert("xzft", record_xzft);
		}
		int pagesCount_xzft = (int)Math.ceil(29.0/DBApp.dataPageSize);
		ArrayList<String> res_xzft = new ArrayList<>();
		for(int i=0;i<pagesCount_xzft;i++)
		{
			res_xzft.add(i+".db");
		}
		Collections.sort(res_xzft);
		String pagesTraceString_xzft = "xzft{ " + String.join(" ",res_xzft);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_xzft+")", fileManagerTrace2.contains(pagesTraceString_xzft));
		//--------------------------------------------------------------------------
		String[] cols3 = {"rkhu2","j4ip","is8","j1h"};
		DBApp.createTable("bdw", cols3);
		for(int i=0;i<100;i++)
		{
			String [] record_bdw = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_bdw[j] = genRandString();
			}
			DBApp.insert("bdw", record_bdw);
		}
		int pagesCount_bdw = (int)Math.ceil(100.0/DBApp.dataPageSize);
		ArrayList<String> res_bdw = new ArrayList<>();
		for(int i=0;i<pagesCount_bdw;i++)
		{
			res_bdw.add(i+".db");
		}
		Collections.sort(res_bdw);
		String pagesTraceString_bdw = "bdw{ " + String.join(" ",res_bdw);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_bdw+")", fileManagerTrace3.contains(pagesTraceString_bdw));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test116TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"kud","w5","o02","x59","yv5t"};
		DBApp.createTable("gy4", cols0);
		for(int i=0;i<60;i++)
		{
			String [] record_gy4 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_gy4[j] = genRandString();
			}
			DBApp.insert("gy4", record_gy4);
		}
		int pagesCount_gy4 = (int)Math.ceil(60.0/DBApp.dataPageSize);
		ArrayList<String> res_gy4 = new ArrayList<>();
		for(int i=0;i<pagesCount_gy4;i++)
		{
			res_gy4.add(i+".db");
		}
		Collections.sort(res_gy4);
		String pagesTraceString_gy4 = "gy4{ " + String.join(" ",res_gy4);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_gy4+")", fileManagerTrace0.contains(pagesTraceString_gy4));
		//--------------------------------------------------------------------------
		String[] cols1 = {"p39","u44"};
		DBApp.createTable("pe0", cols1);
		for(int i=0;i<71;i++)
		{
			String [] record_pe0 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_pe0[j] = genRandString();
			}
			DBApp.insert("pe0", record_pe0);
		}
		int pagesCount_pe0 = (int)Math.ceil(71.0/DBApp.dataPageSize);
		ArrayList<String> res_pe0 = new ArrayList<>();
		for(int i=0;i<pagesCount_pe0;i++)
		{
			res_pe0.add(i+".db");
		}
		Collections.sort(res_pe0);
		String pagesTraceString_pe0 = "pe0{ " + String.join(" ",res_pe0);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_pe0+")", fileManagerTrace1.contains(pagesTraceString_pe0));
		//--------------------------------------------------------------------------
		String[] cols2 = {"kz2","q29","g75mp","kq6","qy","m46"};
		DBApp.createTable("gcb2z", cols2);
		for(int i=0;i<34;i++)
		{
			String [] record_gcb2z = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_gcb2z[j] = genRandString();
			}
			DBApp.insert("gcb2z", record_gcb2z);
		}
		int pagesCount_gcb2z = (int)Math.ceil(34.0/DBApp.dataPageSize);
		ArrayList<String> res_gcb2z = new ArrayList<>();
		for(int i=0;i<pagesCount_gcb2z;i++)
		{
			res_gcb2z.add(i+".db");
		}
		Collections.sort(res_gcb2z);
		String pagesTraceString_gcb2z = "gcb2z{ " + String.join(" ",res_gcb2z);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_gcb2z+")", fileManagerTrace2.contains(pagesTraceString_gcb2z));
		//--------------------------------------------------------------------------
		String[] cols3 = {"gvo9","i1j","k4dt3","r6"};
		DBApp.createTable("a10", cols3);
		for(int i=0;i<69;i++)
		{
			String [] record_a10 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_a10[j] = genRandString();
			}
			DBApp.insert("a10", record_a10);
		}
		int pagesCount_a10 = (int)Math.ceil(69.0/DBApp.dataPageSize);
		ArrayList<String> res_a10 = new ArrayList<>();
		for(int i=0;i<pagesCount_a10;i++)
		{
			res_a10.add(i+".db");
		}
		Collections.sort(res_a10);
		String pagesTraceString_a10 = "a10{ " + String.join(" ",res_a10);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_a10+")", fileManagerTrace3.contains(pagesTraceString_a10));
		//--------------------------------------------------------------------------
		String[] cols4 = {"zm","h67","b4x8z"};
		DBApp.createTable("f1a24", cols4);
		for(int i=0;i<37;i++)
		{
			String [] record_f1a24 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_f1a24[j] = genRandString();
			}
			DBApp.insert("f1a24", record_f1a24);
		}
		int pagesCount_f1a24 = (int)Math.ceil(37.0/DBApp.dataPageSize);
		ArrayList<String> res_f1a24 = new ArrayList<>();
		for(int i=0;i<pagesCount_f1a24;i++)
		{
			res_f1a24.add(i+".db");
		}
		Collections.sort(res_f1a24);
		String pagesTraceString_f1a24 = "f1a24{ " + String.join(" ",res_f1a24);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_f1a24+")", fileManagerTrace4.contains(pagesTraceString_f1a24));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test117TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"li3r","z7z","xc78d","ase6b","bel","q1dts"};
		DBApp.createTable("lr7", cols0);
		for(int i=0;i<5;i++)
		{
			String [] record_lr7 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_lr7[j] = genRandString();
			}
			DBApp.insert("lr7", record_lr7);
		}
		int pagesCount_lr7 = (int)Math.ceil(5.0/DBApp.dataPageSize);
		ArrayList<String> res_lr7 = new ArrayList<>();
		for(int i=0;i<pagesCount_lr7;i++)
		{
			res_lr7.add(i+".db");
		}
		Collections.sort(res_lr7);
		String pagesTraceString_lr7 = "lr7{ " + String.join(" ",res_lr7);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_lr7+")", fileManagerTrace0.contains(pagesTraceString_lr7));
		//--------------------------------------------------------------------------
		String[] cols1 = {"i11","a5q","pm2","y1v","ba8","p8gso","p3","tilw","gj"};
		DBApp.createTable("c5e", cols1);
		for(int i=0;i<19;i++)
		{
			String [] record_c5e = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_c5e[j] = genRandString();
			}
			DBApp.insert("c5e", record_c5e);
		}
		int pagesCount_c5e = (int)Math.ceil(19.0/DBApp.dataPageSize);
		ArrayList<String> res_c5e = new ArrayList<>();
		for(int i=0;i<pagesCount_c5e;i++)
		{
			res_c5e.add(i+".db");
		}
		Collections.sort(res_c5e);
		String pagesTraceString_c5e = "c5e{ " + String.join(" ",res_c5e);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_c5e+")", fileManagerTrace1.contains(pagesTraceString_c5e));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test118TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"fe7em","e79c","vyy11","z3j"};
		DBApp.createTable("cr", cols0);
		for(int i=0;i<85;i++)
		{
			String [] record_cr = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_cr[j] = genRandString();
			}
			DBApp.insert("cr", record_cr);
		}
		int pagesCount_cr = (int)Math.ceil(85.0/DBApp.dataPageSize);
		ArrayList<String> res_cr = new ArrayList<>();
		for(int i=0;i<pagesCount_cr;i++)
		{
			res_cr.add(i+".db");
		}
		Collections.sort(res_cr);
		String pagesTraceString_cr = "cr{ " + String.join(" ",res_cr);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_cr+")", fileManagerTrace0.contains(pagesTraceString_cr));
		//--------------------------------------------------------------------------
		String[] cols1 = {"xx","p8t","lqtp","m29y","pm711","j0ma4"};
		DBApp.createTable("ns", cols1);
		for(int i=0;i<80;i++)
		{
			String [] record_ns = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_ns[j] = genRandString();
			}
			DBApp.insert("ns", record_ns);
		}
		int pagesCount_ns = (int)Math.ceil(80.0/DBApp.dataPageSize);
		ArrayList<String> res_ns = new ArrayList<>();
		for(int i=0;i<pagesCount_ns;i++)
		{
			res_ns.add(i+".db");
		}
		Collections.sort(res_ns);
		String pagesTraceString_ns = "ns{ " + String.join(" ",res_ns);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ns+")", fileManagerTrace1.contains(pagesTraceString_ns));
		//--------------------------------------------------------------------------
		String[] cols2 = {"b6","r6dr1","w8bi","d8j1v","v084h","w8my","o36m9","t91t"};
		DBApp.createTable("x7e3c", cols2);
		for(int i=0;i<28;i++)
		{
			String [] record_x7e3c = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_x7e3c[j] = genRandString();
			}
			DBApp.insert("x7e3c", record_x7e3c);
		}
		int pagesCount_x7e3c = (int)Math.ceil(28.0/DBApp.dataPageSize);
		ArrayList<String> res_x7e3c = new ArrayList<>();
		for(int i=0;i<pagesCount_x7e3c;i++)
		{
			res_x7e3c.add(i+".db");
		}
		Collections.sort(res_x7e3c);
		String pagesTraceString_x7e3c = "x7e3c{ " + String.join(" ",res_x7e3c);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_x7e3c+")", fileManagerTrace2.contains(pagesTraceString_x7e3c));
		//--------------------------------------------------------------------------
		String[] cols3 = {"c2hn3","nx0","eg3kd","u68e","uy16","tai0o"};
		DBApp.createTable("f0e", cols3);
		for(int i=0;i<58;i++)
		{
			String [] record_f0e = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_f0e[j] = genRandString();
			}
			DBApp.insert("f0e", record_f0e);
		}
		int pagesCount_f0e = (int)Math.ceil(58.0/DBApp.dataPageSize);
		ArrayList<String> res_f0e = new ArrayList<>();
		for(int i=0;i<pagesCount_f0e;i++)
		{
			res_f0e.add(i+".db");
		}
		Collections.sort(res_f0e);
		String pagesTraceString_f0e = "f0e{ " + String.join(" ",res_f0e);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_f0e+")", fileManagerTrace3.contains(pagesTraceString_f0e));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test119TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"d34","bm","y83","g263v"};
		DBApp.createTable("j59", cols0);
		for(int i=0;i<32;i++)
		{
			String [] record_j59 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_j59[j] = genRandString();
			}
			DBApp.insert("j59", record_j59);
		}
		int pagesCount_j59 = (int)Math.ceil(32.0/DBApp.dataPageSize);
		ArrayList<String> res_j59 = new ArrayList<>();
		for(int i=0;i<pagesCount_j59;i++)
		{
			res_j59.add(i+".db");
		}
		Collections.sort(res_j59);
		String pagesTraceString_j59 = "j59{ " + String.join(" ",res_j59);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_j59+")", fileManagerTrace0.contains(pagesTraceString_j59));
		//--------------------------------------------------------------------------
		String[] cols1 = {"d2r59","s4"};
		DBApp.createTable("u18", cols1);
		for(int i=0;i<50;i++)
		{
			String [] record_u18 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_u18[j] = genRandString();
			}
			DBApp.insert("u18", record_u18);
		}
		int pagesCount_u18 = (int)Math.ceil(50.0/DBApp.dataPageSize);
		ArrayList<String> res_u18 = new ArrayList<>();
		for(int i=0;i<pagesCount_u18;i++)
		{
			res_u18.add(i+".db");
		}
		Collections.sort(res_u18);
		String pagesTraceString_u18 = "u18{ " + String.join(" ",res_u18);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_u18+")", fileManagerTrace1.contains(pagesTraceString_u18));
		//--------------------------------------------------------------------------
		String[] cols2 = {"w86r","ih","zt"};
		DBApp.createTable("fguvx", cols2);
		for(int i=0;i<63;i++)
		{
			String [] record_fguvx = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_fguvx[j] = genRandString();
			}
			DBApp.insert("fguvx", record_fguvx);
		}
		int pagesCount_fguvx = (int)Math.ceil(63.0/DBApp.dataPageSize);
		ArrayList<String> res_fguvx = new ArrayList<>();
		for(int i=0;i<pagesCount_fguvx;i++)
		{
			res_fguvx.add(i+".db");
		}
		Collections.sort(res_fguvx);
		String pagesTraceString_fguvx = "fguvx{ " + String.join(" ",res_fguvx);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_fguvx+")", fileManagerTrace2.contains(pagesTraceString_fguvx));
		//--------------------------------------------------------------------------
		String[] cols3 = {"kb3","c51t6","j33","nw","hue47","ogdar","f4k","su2","w87"};
		DBApp.createTable("xi9", cols3);
		for(int i=0;i<3;i++)
		{
			String [] record_xi9 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_xi9[j] = genRandString();
			}
			DBApp.insert("xi9", record_xi9);
		}
		int pagesCount_xi9 = (int)Math.ceil(3.0/DBApp.dataPageSize);
		ArrayList<String> res_xi9 = new ArrayList<>();
		for(int i=0;i<pagesCount_xi9;i++)
		{
			res_xi9.add(i+".db");
		}
		Collections.sort(res_xi9);
		String pagesTraceString_xi9 = "xi9{ " + String.join(" ",res_xi9);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_xi9+")", fileManagerTrace3.contains(pagesTraceString_xi9));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test120TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"qk6v7","ffc81","bg6","w13t","ssa72","g01","d8928","h1wn"};
		DBApp.createTable("x1", cols0);
		for(int i=0;i<18;i++)
		{
			String [] record_x1 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_x1[j] = genRandString();
			}
			DBApp.insert("x1", record_x1);
		}
		int pagesCount_x1 = (int)Math.ceil(18.0/DBApp.dataPageSize);
		ArrayList<String> res_x1 = new ArrayList<>();
		for(int i=0;i<pagesCount_x1;i++)
		{
			res_x1.add(i+".db");
		}
		Collections.sort(res_x1);
		String pagesTraceString_x1 = "x1{ " + String.join(" ",res_x1);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_x1+")", fileManagerTrace0.contains(pagesTraceString_x1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"v24","yh","xdf","zv6bv","q8b","b61hq","ow9","bwl85","rs","low8"};
		DBApp.createTable("jehs", cols1);
		for(int i=0;i<38;i++)
		{
			String [] record_jehs = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_jehs[j] = genRandString();
			}
			DBApp.insert("jehs", record_jehs);
		}
		int pagesCount_jehs = (int)Math.ceil(38.0/DBApp.dataPageSize);
		ArrayList<String> res_jehs = new ArrayList<>();
		for(int i=0;i<pagesCount_jehs;i++)
		{
			res_jehs.add(i+".db");
		}
		Collections.sort(res_jehs);
		String pagesTraceString_jehs = "jehs{ " + String.join(" ",res_jehs);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_jehs+")", fileManagerTrace1.contains(pagesTraceString_jehs));
		//--------------------------------------------------------------------------
		String[] cols2 = {"i5","yb8","tj6x","f5v","rw8i","p9","s273"};
		DBApp.createTable("u435", cols2);
		for(int i=0;i<54;i++)
		{
			String [] record_u435 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_u435[j] = genRandString();
			}
			DBApp.insert("u435", record_u435);
		}
		int pagesCount_u435 = (int)Math.ceil(54.0/DBApp.dataPageSize);
		ArrayList<String> res_u435 = new ArrayList<>();
		for(int i=0;i<pagesCount_u435;i++)
		{
			res_u435.add(i+".db");
		}
		Collections.sort(res_u435);
		String pagesTraceString_u435 = "u435{ " + String.join(" ",res_u435);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_u435+")", fileManagerTrace2.contains(pagesTraceString_u435));
		//--------------------------------------------------------------------------
		String[] cols3 = {"ihp","jmu56","c552"};
		DBApp.createTable("q15", cols3);
		for(int i=0;i<45;i++)
		{
			String [] record_q15 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_q15[j] = genRandString();
			}
			DBApp.insert("q15", record_q15);
		}
		int pagesCount_q15 = (int)Math.ceil(45.0/DBApp.dataPageSize);
		ArrayList<String> res_q15 = new ArrayList<>();
		for(int i=0;i<pagesCount_q15;i++)
		{
			res_q15.add(i+".db");
		}
		Collections.sort(res_q15);
		String pagesTraceString_q15 = "q15{ " + String.join(" ",res_q15);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_q15+")", fileManagerTrace3.contains(pagesTraceString_q15));
		//--------------------------------------------------------------------------
		String[] cols4 = {"nuo2"};
		DBApp.createTable("a7wi", cols4);
		for(int i=0;i<62;i++)
		{
			String [] record_a7wi = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_a7wi[j] = genRandString();
			}
			DBApp.insert("a7wi", record_a7wi);
		}
		int pagesCount_a7wi = (int)Math.ceil(62.0/DBApp.dataPageSize);
		ArrayList<String> res_a7wi = new ArrayList<>();
		for(int i=0;i<pagesCount_a7wi;i++)
		{
			res_a7wi.add(i+".db");
		}
		Collections.sort(res_a7wi);
		String pagesTraceString_a7wi = "a7wi{ " + String.join(" ",res_a7wi);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_a7wi+")", fileManagerTrace4.contains(pagesTraceString_a7wi));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test121TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"sr8","ra"};
		DBApp.createTable("g1097", cols0);
		for(int i=0;i<35;i++)
		{
			String [] record_g1097 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_g1097[j] = genRandString();
			}
			DBApp.insert("g1097", record_g1097);
		}
		int pagesCount_g1097 = (int)Math.ceil(35.0/DBApp.dataPageSize);
		ArrayList<String> res_g1097 = new ArrayList<>();
		for(int i=0;i<pagesCount_g1097;i++)
		{
			res_g1097.add(i+".db");
		}
		Collections.sort(res_g1097);
		String pagesTraceString_g1097 = "g1097{ " + String.join(" ",res_g1097);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_g1097+")", fileManagerTrace0.contains(pagesTraceString_g1097));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test122TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"zbw","e57g4","r2i"};
		DBApp.createTable("v6iy", cols0);
		for(int i=0;i<27;i++)
		{
			String [] record_v6iy = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_v6iy[j] = genRandString();
			}
			DBApp.insert("v6iy", record_v6iy);
		}
		int pagesCount_v6iy = (int)Math.ceil(27.0/DBApp.dataPageSize);
		ArrayList<String> res_v6iy = new ArrayList<>();
		for(int i=0;i<pagesCount_v6iy;i++)
		{
			res_v6iy.add(i+".db");
		}
		Collections.sort(res_v6iy);
		String pagesTraceString_v6iy = "v6iy{ " + String.join(" ",res_v6iy);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_v6iy+")", fileManagerTrace0.contains(pagesTraceString_v6iy));
		//--------------------------------------------------------------------------
		String[] cols1 = {"w903","tqm","qn7","ki52"};
		DBApp.createTable("b1", cols1);
		for(int i=0;i<12;i++)
		{
			String [] record_b1 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_b1[j] = genRandString();
			}
			DBApp.insert("b1", record_b1);
		}
		int pagesCount_b1 = (int)Math.ceil(12.0/DBApp.dataPageSize);
		ArrayList<String> res_b1 = new ArrayList<>();
		for(int i=0;i<pagesCount_b1;i++)
		{
			res_b1.add(i+".db");
		}
		Collections.sort(res_b1);
		String pagesTraceString_b1 = "b1{ " + String.join(" ",res_b1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_b1+")", fileManagerTrace1.contains(pagesTraceString_b1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"ei9","zkcjx","i2","tej","i1","v10","l7api","h62q"};
		DBApp.createTable("u7c", cols2);
		for(int i=0;i<41;i++)
		{
			String [] record_u7c = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_u7c[j] = genRandString();
			}
			DBApp.insert("u7c", record_u7c);
		}
		int pagesCount_u7c = (int)Math.ceil(41.0/DBApp.dataPageSize);
		ArrayList<String> res_u7c = new ArrayList<>();
		for(int i=0;i<pagesCount_u7c;i++)
		{
			res_u7c.add(i+".db");
		}
		Collections.sort(res_u7c);
		String pagesTraceString_u7c = "u7c{ " + String.join(" ",res_u7c);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_u7c+")", fileManagerTrace2.contains(pagesTraceString_u7c));
		//--------------------------------------------------------------------------
		String[] cols3 = {"d6"};
		DBApp.createTable("n66", cols3);
		for(int i=0;i<84;i++)
		{
			String [] record_n66 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_n66[j] = genRandString();
			}
			DBApp.insert("n66", record_n66);
		}
		int pagesCount_n66 = (int)Math.ceil(84.0/DBApp.dataPageSize);
		ArrayList<String> res_n66 = new ArrayList<>();
		for(int i=0;i<pagesCount_n66;i++)
		{
			res_n66.add(i+".db");
		}
		Collections.sort(res_n66);
		String pagesTraceString_n66 = "n66{ " + String.join(" ",res_n66);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_n66+")", fileManagerTrace3.contains(pagesTraceString_n66));
		//--------------------------------------------------------------------------
		String[] cols4 = {"x2y","d3j19","r9070","t2","h9p","pqad","y64","ny5"};
		DBApp.createTable("q3h", cols4);
		for(int i=0;i<63;i++)
		{
			String [] record_q3h = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_q3h[j] = genRandString();
			}
			DBApp.insert("q3h", record_q3h);
		}
		int pagesCount_q3h = (int)Math.ceil(63.0/DBApp.dataPageSize);
		ArrayList<String> res_q3h = new ArrayList<>();
		for(int i=0;i<pagesCount_q3h;i++)
		{
			res_q3h.add(i+".db");
		}
		Collections.sort(res_q3h);
		String pagesTraceString_q3h = "q3h{ " + String.join(" ",res_q3h);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_q3h+")", fileManagerTrace4.contains(pagesTraceString_q3h));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test123TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"qlu","k2","vie4"};
		DBApp.createTable("rgm1", cols0);
		for(int i=0;i<55;i++)
		{
			String [] record_rgm1 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_rgm1[j] = genRandString();
			}
			DBApp.insert("rgm1", record_rgm1);
		}
		int pagesCount_rgm1 = (int)Math.ceil(55.0/DBApp.dataPageSize);
		ArrayList<String> res_rgm1 = new ArrayList<>();
		for(int i=0;i<pagesCount_rgm1;i++)
		{
			res_rgm1.add(i+".db");
		}
		Collections.sort(res_rgm1);
		String pagesTraceString_rgm1 = "rgm1{ " + String.join(" ",res_rgm1);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_rgm1+")", fileManagerTrace0.contains(pagesTraceString_rgm1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"j0"};
		DBApp.createTable("uyl", cols1);
		for(int i=0;i<92;i++)
		{
			String [] record_uyl = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_uyl[j] = genRandString();
			}
			DBApp.insert("uyl", record_uyl);
		}
		int pagesCount_uyl = (int)Math.ceil(92.0/DBApp.dataPageSize);
		ArrayList<String> res_uyl = new ArrayList<>();
		for(int i=0;i<pagesCount_uyl;i++)
		{
			res_uyl.add(i+".db");
		}
		Collections.sort(res_uyl);
		String pagesTraceString_uyl = "uyl{ " + String.join(" ",res_uyl);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_uyl+")", fileManagerTrace1.contains(pagesTraceString_uyl));
		//--------------------------------------------------------------------------
		String[] cols2 = {"s645","c669n","dcl","k864","n3","ufd","e3r9"};
		DBApp.createTable("jvba6", cols2);
		for(int i=0;i<1;i++)
		{
			String [] record_jvba6 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_jvba6[j] = genRandString();
			}
			DBApp.insert("jvba6", record_jvba6);
		}
		int pagesCount_jvba6 = (int)Math.ceil(1.0/DBApp.dataPageSize);
		ArrayList<String> res_jvba6 = new ArrayList<>();
		for(int i=0;i<pagesCount_jvba6;i++)
		{
			res_jvba6.add(i+".db");
		}
		Collections.sort(res_jvba6);
		String pagesTraceString_jvba6 = "jvba6{ " + String.join(" ",res_jvba6);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_jvba6+")", fileManagerTrace2.contains(pagesTraceString_jvba6));
		//--------------------------------------------------------------------------
		String[] cols3 = {"zdv6h","z2z0s","u60s"};
		DBApp.createTable("z03", cols3);
		for(int i=0;i<36;i++)
		{
			String [] record_z03 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_z03[j] = genRandString();
			}
			DBApp.insert("z03", record_z03);
		}
		int pagesCount_z03 = (int)Math.ceil(36.0/DBApp.dataPageSize);
		ArrayList<String> res_z03 = new ArrayList<>();
		for(int i=0;i<pagesCount_z03;i++)
		{
			res_z03.add(i+".db");
		}
		Collections.sort(res_z03);
		String pagesTraceString_z03 = "z03{ " + String.join(" ",res_z03);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_z03+")", fileManagerTrace3.contains(pagesTraceString_z03));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test124TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"mk"};
		DBApp.createTable("y5goo", cols0);
		for(int i=0;i<1;i++)
		{
			String [] record_y5goo = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_y5goo[j] = genRandString();
			}
			DBApp.insert("y5goo", record_y5goo);
		}
		int pagesCount_y5goo = (int)Math.ceil(1.0/DBApp.dataPageSize);
		ArrayList<String> res_y5goo = new ArrayList<>();
		for(int i=0;i<pagesCount_y5goo;i++)
		{
			res_y5goo.add(i+".db");
		}
		Collections.sort(res_y5goo);
		String pagesTraceString_y5goo = "y5goo{ " + String.join(" ",res_y5goo);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_y5goo+")", fileManagerTrace0.contains(pagesTraceString_y5goo));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test125TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"xor","rt3v","uq","mj5","q0u49"};
		DBApp.createTable("ej", cols0);
		for(int i=0;i<60;i++)
		{
			String [] record_ej = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ej[j] = genRandString();
			}
			DBApp.insert("ej", record_ej);
		}
		int pagesCount_ej = (int)Math.ceil(60.0/DBApp.dataPageSize);
		ArrayList<String> res_ej = new ArrayList<>();
		for(int i=0;i<pagesCount_ej;i++)
		{
			res_ej.add(i+".db");
		}
		Collections.sort(res_ej);
		String pagesTraceString_ej = "ej{ " + String.join(" ",res_ej);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ej+")", fileManagerTrace0.contains(pagesTraceString_ej));
		//--------------------------------------------------------------------------
		String[] cols1 = {"gs","w326j","op5","nuz","dvo7z"};
		DBApp.createTable("en513", cols1);
		for(int i=0;i<77;i++)
		{
			String [] record_en513 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_en513[j] = genRandString();
			}
			DBApp.insert("en513", record_en513);
		}
		int pagesCount_en513 = (int)Math.ceil(77.0/DBApp.dataPageSize);
		ArrayList<String> res_en513 = new ArrayList<>();
		for(int i=0;i<pagesCount_en513;i++)
		{
			res_en513.add(i+".db");
		}
		Collections.sort(res_en513);
		String pagesTraceString_en513 = "en513{ " + String.join(" ",res_en513);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_en513+")", fileManagerTrace1.contains(pagesTraceString_en513));
		//--------------------------------------------------------------------------
		String[] cols2 = {"dyh","dft","g8113","h83c"};
		DBApp.createTable("f22", cols2);
		for(int i=0;i<16;i++)
		{
			String [] record_f22 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_f22[j] = genRandString();
			}
			DBApp.insert("f22", record_f22);
		}
		int pagesCount_f22 = (int)Math.ceil(16.0/DBApp.dataPageSize);
		ArrayList<String> res_f22 = new ArrayList<>();
		for(int i=0;i<pagesCount_f22;i++)
		{
			res_f22.add(i+".db");
		}
		Collections.sort(res_f22);
		String pagesTraceString_f22 = "f22{ " + String.join(" ",res_f22);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_f22+")", fileManagerTrace2.contains(pagesTraceString_f22));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test126TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"m7","nmcq1","virp","e3p67","e148"};
		DBApp.createTable("x0mj", cols0);
		for(int i=0;i<63;i++)
		{
			String [] record_x0mj = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_x0mj[j] = genRandString();
			}
			DBApp.insert("x0mj", record_x0mj);
		}
		int pagesCount_x0mj = (int)Math.ceil(63.0/DBApp.dataPageSize);
		ArrayList<String> res_x0mj = new ArrayList<>();
		for(int i=0;i<pagesCount_x0mj;i++)
		{
			res_x0mj.add(i+".db");
		}
		Collections.sort(res_x0mj);
		String pagesTraceString_x0mj = "x0mj{ " + String.join(" ",res_x0mj);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_x0mj+")", fileManagerTrace0.contains(pagesTraceString_x0mj));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test127TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"r80","s97"};
		DBApp.createTable("j49q", cols0);
		for(int i=0;i<81;i++)
		{
			String [] record_j49q = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_j49q[j] = genRandString();
			}
			DBApp.insert("j49q", record_j49q);
		}
		int pagesCount_j49q = (int)Math.ceil(81.0/DBApp.dataPageSize);
		ArrayList<String> res_j49q = new ArrayList<>();
		for(int i=0;i<pagesCount_j49q;i++)
		{
			res_j49q.add(i+".db");
		}
		Collections.sort(res_j49q);
		String pagesTraceString_j49q = "j49q{ " + String.join(" ",res_j49q);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_j49q+")", fileManagerTrace0.contains(pagesTraceString_j49q));
		//--------------------------------------------------------------------------
		String[] cols1 = {"r43","s2gu8","mk6","l4","t310","ms34e","ycmfv","rhk0","gg7j8","yr"};
		DBApp.createTable("o3g", cols1);
		for(int i=0;i<25;i++)
		{
			String [] record_o3g = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_o3g[j] = genRandString();
			}
			DBApp.insert("o3g", record_o3g);
		}
		int pagesCount_o3g = (int)Math.ceil(25.0/DBApp.dataPageSize);
		ArrayList<String> res_o3g = new ArrayList<>();
		for(int i=0;i<pagesCount_o3g;i++)
		{
			res_o3g.add(i+".db");
		}
		Collections.sort(res_o3g);
		String pagesTraceString_o3g = "o3g{ " + String.join(" ",res_o3g);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_o3g+")", fileManagerTrace1.contains(pagesTraceString_o3g));
		//--------------------------------------------------------------------------
		String[] cols2 = {"oftns","xq","a22","grm","ad","hs"};
		DBApp.createTable("dq82n", cols2);
		for(int i=0;i<99;i++)
		{
			String [] record_dq82n = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_dq82n[j] = genRandString();
			}
			DBApp.insert("dq82n", record_dq82n);
		}
		int pagesCount_dq82n = (int)Math.ceil(99.0/DBApp.dataPageSize);
		ArrayList<String> res_dq82n = new ArrayList<>();
		for(int i=0;i<pagesCount_dq82n;i++)
		{
			res_dq82n.add(i+".db");
		}
		Collections.sort(res_dq82n);
		String pagesTraceString_dq82n = "dq82n{ " + String.join(" ",res_dq82n);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_dq82n+")", fileManagerTrace2.contains(pagesTraceString_dq82n));
		//--------------------------------------------------------------------------
		String[] cols3 = {"ax8s4","ycr82","b7","pq","o9","u2flr","hp7rg","nw","wlm","m3d1"};
		DBApp.createTable("u29", cols3);
		for(int i=0;i<31;i++)
		{
			String [] record_u29 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_u29[j] = genRandString();
			}
			DBApp.insert("u29", record_u29);
		}
		int pagesCount_u29 = (int)Math.ceil(31.0/DBApp.dataPageSize);
		ArrayList<String> res_u29 = new ArrayList<>();
		for(int i=0;i<pagesCount_u29;i++)
		{
			res_u29.add(i+".db");
		}
		Collections.sort(res_u29);
		String pagesTraceString_u29 = "u29{ " + String.join(" ",res_u29);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_u29+")", fileManagerTrace3.contains(pagesTraceString_u29));
		//--------------------------------------------------------------------------
		String[] cols4 = {"h0gl3","n1akf","v9","u310f"};
		DBApp.createTable("zy96s", cols4);
		for(int i=0;i<87;i++)
		{
			String [] record_zy96s = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_zy96s[j] = genRandString();
			}
			DBApp.insert("zy96s", record_zy96s);
		}
		int pagesCount_zy96s = (int)Math.ceil(87.0/DBApp.dataPageSize);
		ArrayList<String> res_zy96s = new ArrayList<>();
		for(int i=0;i<pagesCount_zy96s;i++)
		{
			res_zy96s.add(i+".db");
		}
		Collections.sort(res_zy96s);
		String pagesTraceString_zy96s = "zy96s{ " + String.join(" ",res_zy96s);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_zy96s+")", fileManagerTrace4.contains(pagesTraceString_zy96s));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test128TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"gkj","mn","ot1m","ge","f7w"};
		DBApp.createTable("a0mv", cols0);
		for(int i=0;i<89;i++)
		{
			String [] record_a0mv = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_a0mv[j] = genRandString();
			}
			DBApp.insert("a0mv", record_a0mv);
		}
		int pagesCount_a0mv = (int)Math.ceil(89.0/DBApp.dataPageSize);
		ArrayList<String> res_a0mv = new ArrayList<>();
		for(int i=0;i<pagesCount_a0mv;i++)
		{
			res_a0mv.add(i+".db");
		}
		Collections.sort(res_a0mv);
		String pagesTraceString_a0mv = "a0mv{ " + String.join(" ",res_a0mv);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_a0mv+")", fileManagerTrace0.contains(pagesTraceString_a0mv));
		//--------------------------------------------------------------------------
		String[] cols1 = {"g09e9"};
		DBApp.createTable("nr7", cols1);
		for(int i=0;i<56;i++)
		{
			String [] record_nr7 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_nr7[j] = genRandString();
			}
			DBApp.insert("nr7", record_nr7);
		}
		int pagesCount_nr7 = (int)Math.ceil(56.0/DBApp.dataPageSize);
		ArrayList<String> res_nr7 = new ArrayList<>();
		for(int i=0;i<pagesCount_nr7;i++)
		{
			res_nr7.add(i+".db");
		}
		Collections.sort(res_nr7);
		String pagesTraceString_nr7 = "nr7{ " + String.join(" ",res_nr7);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_nr7+")", fileManagerTrace1.contains(pagesTraceString_nr7));
		//--------------------------------------------------------------------------
		String[] cols2 = {"j4","vp3","ucm8","wet","e9","y6b6","g0r","p7o","k62","kc3j6"};
		DBApp.createTable("guid", cols2);
		for(int i=0;i<51;i++)
		{
			String [] record_guid = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_guid[j] = genRandString();
			}
			DBApp.insert("guid", record_guid);
		}
		int pagesCount_guid = (int)Math.ceil(51.0/DBApp.dataPageSize);
		ArrayList<String> res_guid = new ArrayList<>();
		for(int i=0;i<pagesCount_guid;i++)
		{
			res_guid.add(i+".db");
		}
		Collections.sort(res_guid);
		String pagesTraceString_guid = "guid{ " + String.join(" ",res_guid);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_guid+")", fileManagerTrace2.contains(pagesTraceString_guid));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test129TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"nkqa9"};
		DBApp.createTable("u3v", cols0);
		for(int i=0;i<86;i++)
		{
			String [] record_u3v = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_u3v[j] = genRandString();
			}
			DBApp.insert("u3v", record_u3v);
		}
		int pagesCount_u3v = (int)Math.ceil(86.0/DBApp.dataPageSize);
		ArrayList<String> res_u3v = new ArrayList<>();
		for(int i=0;i<pagesCount_u3v;i++)
		{
			res_u3v.add(i+".db");
		}
		Collections.sort(res_u3v);
		String pagesTraceString_u3v = "u3v{ " + String.join(" ",res_u3v);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_u3v+")", fileManagerTrace0.contains(pagesTraceString_u3v));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test130TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"gos","fvyv","xuz4w","di","rpn","jc","objsg"};
		DBApp.createTable("t2r9k", cols0);
		for(int i=0;i<68;i++)
		{
			String [] record_t2r9k = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_t2r9k[j] = genRandString();
			}
			DBApp.insert("t2r9k", record_t2r9k);
		}
		int pagesCount_t2r9k = (int)Math.ceil(68.0/DBApp.dataPageSize);
		ArrayList<String> res_t2r9k = new ArrayList<>();
		for(int i=0;i<pagesCount_t2r9k;i++)
		{
			res_t2r9k.add(i+".db");
		}
		Collections.sort(res_t2r9k);
		String pagesTraceString_t2r9k = "t2r9k{ " + String.join(" ",res_t2r9k);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_t2r9k+")", fileManagerTrace0.contains(pagesTraceString_t2r9k));
		//--------------------------------------------------------------------------
		String[] cols1 = {"d92s","px","xqacw","hjb","z37","yp","m9g5w","qk9"};
		DBApp.createTable("n47", cols1);
		for(int i=0;i<4;i++)
		{
			String [] record_n47 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_n47[j] = genRandString();
			}
			DBApp.insert("n47", record_n47);
		}
		int pagesCount_n47 = (int)Math.ceil(4.0/DBApp.dataPageSize);
		ArrayList<String> res_n47 = new ArrayList<>();
		for(int i=0;i<pagesCount_n47;i++)
		{
			res_n47.add(i+".db");
		}
		Collections.sort(res_n47);
		String pagesTraceString_n47 = "n47{ " + String.join(" ",res_n47);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_n47+")", fileManagerTrace1.contains(pagesTraceString_n47));
		//--------------------------------------------------------------------------
		String[] cols2 = {"fgdkl","t33","j50f","t0qi4","j9pam","bpy","rp9s","xa","cr"};
		DBApp.createTable("mtuf", cols2);
		for(int i=0;i<48;i++)
		{
			String [] record_mtuf = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_mtuf[j] = genRandString();
			}
			DBApp.insert("mtuf", record_mtuf);
		}
		int pagesCount_mtuf = (int)Math.ceil(48.0/DBApp.dataPageSize);
		ArrayList<String> res_mtuf = new ArrayList<>();
		for(int i=0;i<pagesCount_mtuf;i++)
		{
			res_mtuf.add(i+".db");
		}
		Collections.sort(res_mtuf);
		String pagesTraceString_mtuf = "mtuf{ " + String.join(" ",res_mtuf);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_mtuf+")", fileManagerTrace2.contains(pagesTraceString_mtuf));
		//--------------------------------------------------------------------------
		String[] cols3 = {"rk","gxt","p0","tpc","qq","xme3u","be2h","zk1","b68"};
		DBApp.createTable("c3n", cols3);
		for(int i=0;i<90;i++)
		{
			String [] record_c3n = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_c3n[j] = genRandString();
			}
			DBApp.insert("c3n", record_c3n);
		}
		int pagesCount_c3n = (int)Math.ceil(90.0/DBApp.dataPageSize);
		ArrayList<String> res_c3n = new ArrayList<>();
		for(int i=0;i<pagesCount_c3n;i++)
		{
			res_c3n.add(i+".db");
		}
		Collections.sort(res_c3n);
		String pagesTraceString_c3n = "c3n{ " + String.join(" ",res_c3n);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_c3n+")", fileManagerTrace3.contains(pagesTraceString_c3n));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test131TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"foz","d2"};
		DBApp.createTable("iou0", cols0);
		for(int i=0;i<43;i++)
		{
			String [] record_iou0 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_iou0[j] = genRandString();
			}
			DBApp.insert("iou0", record_iou0);
		}
		int pagesCount_iou0 = (int)Math.ceil(43.0/DBApp.dataPageSize);
		ArrayList<String> res_iou0 = new ArrayList<>();
		for(int i=0;i<pagesCount_iou0;i++)
		{
			res_iou0.add(i+".db");
		}
		Collections.sort(res_iou0);
		String pagesTraceString_iou0 = "iou0{ " + String.join(" ",res_iou0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_iou0+")", fileManagerTrace0.contains(pagesTraceString_iou0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"o0l7r","os2","d2","rkn11","d5ww0","g3j","s54","e721"};
		DBApp.createTable("m8l4s", cols1);
		for(int i=0;i<35;i++)
		{
			String [] record_m8l4s = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_m8l4s[j] = genRandString();
			}
			DBApp.insert("m8l4s", record_m8l4s);
		}
		int pagesCount_m8l4s = (int)Math.ceil(35.0/DBApp.dataPageSize);
		ArrayList<String> res_m8l4s = new ArrayList<>();
		for(int i=0;i<pagesCount_m8l4s;i++)
		{
			res_m8l4s.add(i+".db");
		}
		Collections.sort(res_m8l4s);
		String pagesTraceString_m8l4s = "m8l4s{ " + String.join(" ",res_m8l4s);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_m8l4s+")", fileManagerTrace1.contains(pagesTraceString_m8l4s));
		//--------------------------------------------------------------------------
		String[] cols2 = {"ba","u305","n61","lw","z30","re0b","f47"};
		DBApp.createTable("msu", cols2);
		for(int i=0;i<63;i++)
		{
			String [] record_msu = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_msu[j] = genRandString();
			}
			DBApp.insert("msu", record_msu);
		}
		int pagesCount_msu = (int)Math.ceil(63.0/DBApp.dataPageSize);
		ArrayList<String> res_msu = new ArrayList<>();
		for(int i=0;i<pagesCount_msu;i++)
		{
			res_msu.add(i+".db");
		}
		Collections.sort(res_msu);
		String pagesTraceString_msu = "msu{ " + String.join(" ",res_msu);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_msu+")", fileManagerTrace2.contains(pagesTraceString_msu));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test132TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"m4b9","w38"};
		DBApp.createTable("wm42j", cols0);
		for(int i=0;i<88;i++)
		{
			String [] record_wm42j = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_wm42j[j] = genRandString();
			}
			DBApp.insert("wm42j", record_wm42j);
		}
		int pagesCount_wm42j = (int)Math.ceil(88.0/DBApp.dataPageSize);
		ArrayList<String> res_wm42j = new ArrayList<>();
		for(int i=0;i<pagesCount_wm42j;i++)
		{
			res_wm42j.add(i+".db");
		}
		Collections.sort(res_wm42j);
		String pagesTraceString_wm42j = "wm42j{ " + String.join(" ",res_wm42j);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_wm42j+")", fileManagerTrace0.contains(pagesTraceString_wm42j));
		//--------------------------------------------------------------------------
		String[] cols1 = {"s14","eahn","h7","cdrr3","f2","wp7s","bdc","fn"};
		DBApp.createTable("fc84", cols1);
		for(int i=0;i<41;i++)
		{
			String [] record_fc84 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_fc84[j] = genRandString();
			}
			DBApp.insert("fc84", record_fc84);
		}
		int pagesCount_fc84 = (int)Math.ceil(41.0/DBApp.dataPageSize);
		ArrayList<String> res_fc84 = new ArrayList<>();
		for(int i=0;i<pagesCount_fc84;i++)
		{
			res_fc84.add(i+".db");
		}
		Collections.sort(res_fc84);
		String pagesTraceString_fc84 = "fc84{ " + String.join(" ",res_fc84);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_fc84+")", fileManagerTrace1.contains(pagesTraceString_fc84));
		//--------------------------------------------------------------------------
		String[] cols2 = {"jp7jk"};
		DBApp.createTable("b4", cols2);
		for(int i=0;i<58;i++)
		{
			String [] record_b4 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_b4[j] = genRandString();
			}
			DBApp.insert("b4", record_b4);
		}
		int pagesCount_b4 = (int)Math.ceil(58.0/DBApp.dataPageSize);
		ArrayList<String> res_b4 = new ArrayList<>();
		for(int i=0;i<pagesCount_b4;i++)
		{
			res_b4.add(i+".db");
		}
		Collections.sort(res_b4);
		String pagesTraceString_b4 = "b4{ " + String.join(" ",res_b4);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_b4+")", fileManagerTrace2.contains(pagesTraceString_b4));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test133TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"mug8"};
		DBApp.createTable("zuk2", cols0);
		for(int i=0;i<77;i++)
		{
			String [] record_zuk2 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_zuk2[j] = genRandString();
			}
			DBApp.insert("zuk2", record_zuk2);
		}
		int pagesCount_zuk2 = (int)Math.ceil(77.0/DBApp.dataPageSize);
		ArrayList<String> res_zuk2 = new ArrayList<>();
		for(int i=0;i<pagesCount_zuk2;i++)
		{
			res_zuk2.add(i+".db");
		}
		Collections.sort(res_zuk2);
		String pagesTraceString_zuk2 = "zuk2{ " + String.join(" ",res_zuk2);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_zuk2+")", fileManagerTrace0.contains(pagesTraceString_zuk2));
		//--------------------------------------------------------------------------
		String[] cols1 = {"bbd","v2","f97","okik","zlt","di0"};
		DBApp.createTable("jm2v", cols1);
		for(int i=0;i<5;i++)
		{
			String [] record_jm2v = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_jm2v[j] = genRandString();
			}
			DBApp.insert("jm2v", record_jm2v);
		}
		int pagesCount_jm2v = (int)Math.ceil(5.0/DBApp.dataPageSize);
		ArrayList<String> res_jm2v = new ArrayList<>();
		for(int i=0;i<pagesCount_jm2v;i++)
		{
			res_jm2v.add(i+".db");
		}
		Collections.sort(res_jm2v);
		String pagesTraceString_jm2v = "jm2v{ " + String.join(" ",res_jm2v);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_jm2v+")", fileManagerTrace1.contains(pagesTraceString_jm2v));
		//--------------------------------------------------------------------------
		String[] cols2 = {"k3","bz","me","eg5","b6","x26","h3le","v3d","jpqb"};
		DBApp.createTable("i0", cols2);
		for(int i=0;i<37;i++)
		{
			String [] record_i0 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_i0[j] = genRandString();
			}
			DBApp.insert("i0", record_i0);
		}
		int pagesCount_i0 = (int)Math.ceil(37.0/DBApp.dataPageSize);
		ArrayList<String> res_i0 = new ArrayList<>();
		for(int i=0;i<pagesCount_i0;i++)
		{
			res_i0.add(i+".db");
		}
		Collections.sort(res_i0);
		String pagesTraceString_i0 = "i0{ " + String.join(" ",res_i0);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_i0+")", fileManagerTrace2.contains(pagesTraceString_i0));
		//--------------------------------------------------------------------------
		String[] cols3 = {"w120","e1r"};
		DBApp.createTable("wcj", cols3);
		for(int i=0;i<24;i++)
		{
			String [] record_wcj = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_wcj[j] = genRandString();
			}
			DBApp.insert("wcj", record_wcj);
		}
		int pagesCount_wcj = (int)Math.ceil(24.0/DBApp.dataPageSize);
		ArrayList<String> res_wcj = new ArrayList<>();
		for(int i=0;i<pagesCount_wcj;i++)
		{
			res_wcj.add(i+".db");
		}
		Collections.sort(res_wcj);
		String pagesTraceString_wcj = "wcj{ " + String.join(" ",res_wcj);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_wcj+")", fileManagerTrace3.contains(pagesTraceString_wcj));
		//--------------------------------------------------------------------------
		String[] cols4 = {"wie","k611","qik","kz4","f4yj5","kgu","s21","s6vc6","mgp"};
		DBApp.createTable("ld", cols4);
		for(int i=0;i<53;i++)
		{
			String [] record_ld = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_ld[j] = genRandString();
			}
			DBApp.insert("ld", record_ld);
		}
		int pagesCount_ld = (int)Math.ceil(53.0/DBApp.dataPageSize);
		ArrayList<String> res_ld = new ArrayList<>();
		for(int i=0;i<pagesCount_ld;i++)
		{
			res_ld.add(i+".db");
		}
		Collections.sort(res_ld);
		String pagesTraceString_ld = "ld{ " + String.join(" ",res_ld);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ld+")", fileManagerTrace4.contains(pagesTraceString_ld));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test134TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"f7m","bpk","og4ci","jxd","h3","kq5","p3","c9y","n07v","op"};
		DBApp.createTable("l60z", cols0);
		for(int i=0;i<15;i++)
		{
			String [] record_l60z = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_l60z[j] = genRandString();
			}
			DBApp.insert("l60z", record_l60z);
		}
		int pagesCount_l60z = (int)Math.ceil(15.0/DBApp.dataPageSize);
		ArrayList<String> res_l60z = new ArrayList<>();
		for(int i=0;i<pagesCount_l60z;i++)
		{
			res_l60z.add(i+".db");
		}
		Collections.sort(res_l60z);
		String pagesTraceString_l60z = "l60z{ " + String.join(" ",res_l60z);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_l60z+")", fileManagerTrace0.contains(pagesTraceString_l60z));
		//--------------------------------------------------------------------------
		String[] cols1 = {"pp","v2","gz","o377","s1f","c67","hy9"};
		DBApp.createTable("sg56", cols1);
		for(int i=0;i<94;i++)
		{
			String [] record_sg56 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_sg56[j] = genRandString();
			}
			DBApp.insert("sg56", record_sg56);
		}
		int pagesCount_sg56 = (int)Math.ceil(94.0/DBApp.dataPageSize);
		ArrayList<String> res_sg56 = new ArrayList<>();
		for(int i=0;i<pagesCount_sg56;i++)
		{
			res_sg56.add(i+".db");
		}
		Collections.sort(res_sg56);
		String pagesTraceString_sg56 = "sg56{ " + String.join(" ",res_sg56);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_sg56+")", fileManagerTrace1.contains(pagesTraceString_sg56));
		//--------------------------------------------------------------------------
		String[] cols2 = {"v44t6"};
		DBApp.createTable("ni47c", cols2);
		for(int i=0;i<32;i++)
		{
			String [] record_ni47c = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_ni47c[j] = genRandString();
			}
			DBApp.insert("ni47c", record_ni47c);
		}
		int pagesCount_ni47c = (int)Math.ceil(32.0/DBApp.dataPageSize);
		ArrayList<String> res_ni47c = new ArrayList<>();
		for(int i=0;i<pagesCount_ni47c;i++)
		{
			res_ni47c.add(i+".db");
		}
		Collections.sort(res_ni47c);
		String pagesTraceString_ni47c = "ni47c{ " + String.join(" ",res_ni47c);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ni47c+")", fileManagerTrace2.contains(pagesTraceString_ni47c));
		//--------------------------------------------------------------------------
		String[] cols3 = {"z5cg5","vc34j","y54","b5g5d","k3","mjh","ywm8"};
		DBApp.createTable("mwyk4", cols3);
		for(int i=0;i<54;i++)
		{
			String [] record_mwyk4 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_mwyk4[j] = genRandString();
			}
			DBApp.insert("mwyk4", record_mwyk4);
		}
		int pagesCount_mwyk4 = (int)Math.ceil(54.0/DBApp.dataPageSize);
		ArrayList<String> res_mwyk4 = new ArrayList<>();
		for(int i=0;i<pagesCount_mwyk4;i++)
		{
			res_mwyk4.add(i+".db");
		}
		Collections.sort(res_mwyk4);
		String pagesTraceString_mwyk4 = "mwyk4{ " + String.join(" ",res_mwyk4);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_mwyk4+")", fileManagerTrace3.contains(pagesTraceString_mwyk4));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test135TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"y9","e6k","wh4b","q89","gv4","e8","wva","rx"};
		DBApp.createTable("any", cols0);
		for(int i=0;i<78;i++)
		{
			String [] record_any = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_any[j] = genRandString();
			}
			DBApp.insert("any", record_any);
		}
		int pagesCount_any = (int)Math.ceil(78.0/DBApp.dataPageSize);
		ArrayList<String> res_any = new ArrayList<>();
		for(int i=0;i<pagesCount_any;i++)
		{
			res_any.add(i+".db");
		}
		Collections.sort(res_any);
		String pagesTraceString_any = "any{ " + String.join(" ",res_any);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_any+")", fileManagerTrace0.contains(pagesTraceString_any));
		//--------------------------------------------------------------------------
		String[] cols1 = {"uke","tvv38","uj8ef","v54","i6","hi5"};
		DBApp.createTable("cy", cols1);
		for(int i=0;i<18;i++)
		{
			String [] record_cy = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_cy[j] = genRandString();
			}
			DBApp.insert("cy", record_cy);
		}
		int pagesCount_cy = (int)Math.ceil(18.0/DBApp.dataPageSize);
		ArrayList<String> res_cy = new ArrayList<>();
		for(int i=0;i<pagesCount_cy;i++)
		{
			res_cy.add(i+".db");
		}
		Collections.sort(res_cy);
		String pagesTraceString_cy = "cy{ " + String.join(" ",res_cy);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_cy+")", fileManagerTrace1.contains(pagesTraceString_cy));
		//--------------------------------------------------------------------------
		String[] cols2 = {"thz9o","tmh","kf2","mc","qwtz1","z5m","s6h","h4r1","jr10","u093"};
		DBApp.createTable("wu4c", cols2);
		for(int i=0;i<94;i++)
		{
			String [] record_wu4c = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_wu4c[j] = genRandString();
			}
			DBApp.insert("wu4c", record_wu4c);
		}
		int pagesCount_wu4c = (int)Math.ceil(94.0/DBApp.dataPageSize);
		ArrayList<String> res_wu4c = new ArrayList<>();
		for(int i=0;i<pagesCount_wu4c;i++)
		{
			res_wu4c.add(i+".db");
		}
		Collections.sort(res_wu4c);
		String pagesTraceString_wu4c = "wu4c{ " + String.join(" ",res_wu4c);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_wu4c+")", fileManagerTrace2.contains(pagesTraceString_wu4c));
		//--------------------------------------------------------------------------
		String[] cols3 = {"wgo","n97z3","u1","xh"};
		DBApp.createTable("ggh", cols3);
		for(int i=0;i<83;i++)
		{
			String [] record_ggh = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_ggh[j] = genRandString();
			}
			DBApp.insert("ggh", record_ggh);
		}
		int pagesCount_ggh = (int)Math.ceil(83.0/DBApp.dataPageSize);
		ArrayList<String> res_ggh = new ArrayList<>();
		for(int i=0;i<pagesCount_ggh;i++)
		{
			res_ggh.add(i+".db");
		}
		Collections.sort(res_ggh);
		String pagesTraceString_ggh = "ggh{ " + String.join(" ",res_ggh);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ggh+")", fileManagerTrace3.contains(pagesTraceString_ggh));
		//--------------------------------------------------------------------------
		String[] cols4 = {"x8u","d93bl","w65","uu7","j434","j9","i7","sfv","utp"};
		DBApp.createTable("s7k", cols4);
		for(int i=0;i<64;i++)
		{
			String [] record_s7k = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_s7k[j] = genRandString();
			}
			DBApp.insert("s7k", record_s7k);
		}
		int pagesCount_s7k = (int)Math.ceil(64.0/DBApp.dataPageSize);
		ArrayList<String> res_s7k = new ArrayList<>();
		for(int i=0;i<pagesCount_s7k;i++)
		{
			res_s7k.add(i+".db");
		}
		Collections.sort(res_s7k);
		String pagesTraceString_s7k = "s7k{ " + String.join(" ",res_s7k);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_s7k+")", fileManagerTrace4.contains(pagesTraceString_s7k));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test136TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"vfj2","p21c","c2x","y8","qtl","a1","frw","w071u"};
		DBApp.createTable("rh2", cols0);
		for(int i=0;i<37;i++)
		{
			String [] record_rh2 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_rh2[j] = genRandString();
			}
			DBApp.insert("rh2", record_rh2);
		}
		int pagesCount_rh2 = (int)Math.ceil(37.0/DBApp.dataPageSize);
		ArrayList<String> res_rh2 = new ArrayList<>();
		for(int i=0;i<pagesCount_rh2;i++)
		{
			res_rh2.add(i+".db");
		}
		Collections.sort(res_rh2);
		String pagesTraceString_rh2 = "rh2{ " + String.join(" ",res_rh2);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_rh2+")", fileManagerTrace0.contains(pagesTraceString_rh2));
		//--------------------------------------------------------------------------
		String[] cols1 = {"e5z","qqw1x"};
		DBApp.createTable("m24yx", cols1);
		for(int i=0;i<51;i++)
		{
			String [] record_m24yx = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_m24yx[j] = genRandString();
			}
			DBApp.insert("m24yx", record_m24yx);
		}
		int pagesCount_m24yx = (int)Math.ceil(51.0/DBApp.dataPageSize);
		ArrayList<String> res_m24yx = new ArrayList<>();
		for(int i=0;i<pagesCount_m24yx;i++)
		{
			res_m24yx.add(i+".db");
		}
		Collections.sort(res_m24yx);
		String pagesTraceString_m24yx = "m24yx{ " + String.join(" ",res_m24yx);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_m24yx+")", fileManagerTrace1.contains(pagesTraceString_m24yx));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test137TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"jq7","y2ec","xx5","lc","tio","zvw","b19"};
		DBApp.createTable("km7", cols0);
		for(int i=0;i<22;i++)
		{
			String [] record_km7 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_km7[j] = genRandString();
			}
			DBApp.insert("km7", record_km7);
		}
		int pagesCount_km7 = (int)Math.ceil(22.0/DBApp.dataPageSize);
		ArrayList<String> res_km7 = new ArrayList<>();
		for(int i=0;i<pagesCount_km7;i++)
		{
			res_km7.add(i+".db");
		}
		Collections.sort(res_km7);
		String pagesTraceString_km7 = "km7{ " + String.join(" ",res_km7);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_km7+")", fileManagerTrace0.contains(pagesTraceString_km7));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ds99","i4c","yex","w99","rjpl0"};
		DBApp.createTable("wm701", cols1);
		for(int i=0;i<9;i++)
		{
			String [] record_wm701 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_wm701[j] = genRandString();
			}
			DBApp.insert("wm701", record_wm701);
		}
		int pagesCount_wm701 = (int)Math.ceil(9.0/DBApp.dataPageSize);
		ArrayList<String> res_wm701 = new ArrayList<>();
		for(int i=0;i<pagesCount_wm701;i++)
		{
			res_wm701.add(i+".db");
		}
		Collections.sort(res_wm701);
		String pagesTraceString_wm701 = "wm701{ " + String.join(" ",res_wm701);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_wm701+")", fileManagerTrace1.contains(pagesTraceString_wm701));
		//--------------------------------------------------------------------------
		String[] cols2 = {"scw1","h1","mom","m1i1","h04"};
		DBApp.createTable("fd1s3", cols2);
		for(int i=0;i<60;i++)
		{
			String [] record_fd1s3 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_fd1s3[j] = genRandString();
			}
			DBApp.insert("fd1s3", record_fd1s3);
		}
		int pagesCount_fd1s3 = (int)Math.ceil(60.0/DBApp.dataPageSize);
		ArrayList<String> res_fd1s3 = new ArrayList<>();
		for(int i=0;i<pagesCount_fd1s3;i++)
		{
			res_fd1s3.add(i+".db");
		}
		Collections.sort(res_fd1s3);
		String pagesTraceString_fd1s3 = "fd1s3{ " + String.join(" ",res_fd1s3);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_fd1s3+")", fileManagerTrace2.contains(pagesTraceString_fd1s3));
		//--------------------------------------------------------------------------
		String[] cols3 = {"e5e4","e5155","sj1"};
		DBApp.createTable("ymg3", cols3);
		for(int i=0;i<97;i++)
		{
			String [] record_ymg3 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_ymg3[j] = genRandString();
			}
			DBApp.insert("ymg3", record_ymg3);
		}
		int pagesCount_ymg3 = (int)Math.ceil(97.0/DBApp.dataPageSize);
		ArrayList<String> res_ymg3 = new ArrayList<>();
		for(int i=0;i<pagesCount_ymg3;i++)
		{
			res_ymg3.add(i+".db");
		}
		Collections.sort(res_ymg3);
		String pagesTraceString_ymg3 = "ymg3{ " + String.join(" ",res_ymg3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ymg3+")", fileManagerTrace3.contains(pagesTraceString_ymg3));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test138TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"t7u","z58","dy3","tj5","z16","d62","vu0","o89"};
		DBApp.createTable("l7", cols0);
		for(int i=0;i<69;i++)
		{
			String [] record_l7 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_l7[j] = genRandString();
			}
			DBApp.insert("l7", record_l7);
		}
		int pagesCount_l7 = (int)Math.ceil(69.0/DBApp.dataPageSize);
		ArrayList<String> res_l7 = new ArrayList<>();
		for(int i=0;i<pagesCount_l7;i++)
		{
			res_l7.add(i+".db");
		}
		Collections.sort(res_l7);
		String pagesTraceString_l7 = "l7{ " + String.join(" ",res_l7);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_l7+")", fileManagerTrace0.contains(pagesTraceString_l7));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test139TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"s4","uw6nd","w23","sh","zv","xr9"};
		DBApp.createTable("j5", cols0);
		for(int i=0;i<69;i++)
		{
			String [] record_j5 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_j5[j] = genRandString();
			}
			DBApp.insert("j5", record_j5);
		}
		int pagesCount_j5 = (int)Math.ceil(69.0/DBApp.dataPageSize);
		ArrayList<String> res_j5 = new ArrayList<>();
		for(int i=0;i<pagesCount_j5;i++)
		{
			res_j5.add(i+".db");
		}
		Collections.sort(res_j5);
		String pagesTraceString_j5 = "j5{ " + String.join(" ",res_j5);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_j5+")", fileManagerTrace0.contains(pagesTraceString_j5));
		//--------------------------------------------------------------------------
		String[] cols1 = {"sen","o6mnj","kv04","pxz","em2","be889","r89","yt0"};
		DBApp.createTable("j47", cols1);
		for(int i=0;i<94;i++)
		{
			String [] record_j47 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_j47[j] = genRandString();
			}
			DBApp.insert("j47", record_j47);
		}
		int pagesCount_j47 = (int)Math.ceil(94.0/DBApp.dataPageSize);
		ArrayList<String> res_j47 = new ArrayList<>();
		for(int i=0;i<pagesCount_j47;i++)
		{
			res_j47.add(i+".db");
		}
		Collections.sort(res_j47);
		String pagesTraceString_j47 = "j47{ " + String.join(" ",res_j47);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_j47+")", fileManagerTrace1.contains(pagesTraceString_j47));
		//--------------------------------------------------------------------------
		String[] cols2 = {"i2n91","c2t","ksq","ilri","gar54","w3u"};
		DBApp.createTable("y9", cols2);
		for(int i=0;i<42;i++)
		{
			String [] record_y9 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_y9[j] = genRandString();
			}
			DBApp.insert("y9", record_y9);
		}
		int pagesCount_y9 = (int)Math.ceil(42.0/DBApp.dataPageSize);
		ArrayList<String> res_y9 = new ArrayList<>();
		for(int i=0;i<pagesCount_y9;i++)
		{
			res_y9.add(i+".db");
		}
		Collections.sort(res_y9);
		String pagesTraceString_y9 = "y9{ " + String.join(" ",res_y9);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_y9+")", fileManagerTrace2.contains(pagesTraceString_y9));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test140TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"d58u","pu6y","s9x","g47vz"};
		DBApp.createTable("t43", cols0);
		for(int i=0;i<1;i++)
		{
			String [] record_t43 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_t43[j] = genRandString();
			}
			DBApp.insert("t43", record_t43);
		}
		int pagesCount_t43 = (int)Math.ceil(1.0/DBApp.dataPageSize);
		ArrayList<String> res_t43 = new ArrayList<>();
		for(int i=0;i<pagesCount_t43;i++)
		{
			res_t43.add(i+".db");
		}
		Collections.sort(res_t43);
		String pagesTraceString_t43 = "t43{ " + String.join(" ",res_t43);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_t43+")", fileManagerTrace0.contains(pagesTraceString_t43));
		//--------------------------------------------------------------------------
		String[] cols1 = {"s39","e76","ln0","t8gk"};
		DBApp.createTable("jb0", cols1);
		for(int i=0;i<86;i++)
		{
			String [] record_jb0 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_jb0[j] = genRandString();
			}
			DBApp.insert("jb0", record_jb0);
		}
		int pagesCount_jb0 = (int)Math.ceil(86.0/DBApp.dataPageSize);
		ArrayList<String> res_jb0 = new ArrayList<>();
		for(int i=0;i<pagesCount_jb0;i++)
		{
			res_jb0.add(i+".db");
		}
		Collections.sort(res_jb0);
		String pagesTraceString_jb0 = "jb0{ " + String.join(" ",res_jb0);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_jb0+")", fileManagerTrace1.contains(pagesTraceString_jb0));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test141TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"phy89","w339j","jxv","d4","rb9m","t5q9","jll"};
		DBApp.createTable("h0", cols0);
		for(int i=0;i<48;i++)
		{
			String [] record_h0 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_h0[j] = genRandString();
			}
			DBApp.insert("h0", record_h0);
		}
		int pagesCount_h0 = (int)Math.ceil(48.0/DBApp.dataPageSize);
		ArrayList<String> res_h0 = new ArrayList<>();
		for(int i=0;i<pagesCount_h0;i++)
		{
			res_h0.add(i+".db");
		}
		Collections.sort(res_h0);
		String pagesTraceString_h0 = "h0{ " + String.join(" ",res_h0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_h0+")", fileManagerTrace0.contains(pagesTraceString_h0));
		//--------------------------------------------------------------------------
		String[] cols1 = {"u15","j8i","s6","u19","uaa","k20"};
		DBApp.createTable("m39q", cols1);
		for(int i=0;i<30;i++)
		{
			String [] record_m39q = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_m39q[j] = genRandString();
			}
			DBApp.insert("m39q", record_m39q);
		}
		int pagesCount_m39q = (int)Math.ceil(30.0/DBApp.dataPageSize);
		ArrayList<String> res_m39q = new ArrayList<>();
		for(int i=0;i<pagesCount_m39q;i++)
		{
			res_m39q.add(i+".db");
		}
		Collections.sort(res_m39q);
		String pagesTraceString_m39q = "m39q{ " + String.join(" ",res_m39q);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_m39q+")", fileManagerTrace1.contains(pagesTraceString_m39q));
		//--------------------------------------------------------------------------
		String[] cols2 = {"po8p3","k4933","m43o"};
		DBApp.createTable("t3", cols2);
		for(int i=0;i<24;i++)
		{
			String [] record_t3 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_t3[j] = genRandString();
			}
			DBApp.insert("t3", record_t3);
		}
		int pagesCount_t3 = (int)Math.ceil(24.0/DBApp.dataPageSize);
		ArrayList<String> res_t3 = new ArrayList<>();
		for(int i=0;i<pagesCount_t3;i++)
		{
			res_t3.add(i+".db");
		}
		Collections.sort(res_t3);
		String pagesTraceString_t3 = "t3{ " + String.join(" ",res_t3);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_t3+")", fileManagerTrace2.contains(pagesTraceString_t3));
		//--------------------------------------------------------------------------
		String[] cols3 = {"o0","wyrs","lvv","a88ap"};
		DBApp.createTable("r2", cols3);
		for(int i=0;i<83;i++)
		{
			String [] record_r2 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_r2[j] = genRandString();
			}
			DBApp.insert("r2", record_r2);
		}
		int pagesCount_r2 = (int)Math.ceil(83.0/DBApp.dataPageSize);
		ArrayList<String> res_r2 = new ArrayList<>();
		for(int i=0;i<pagesCount_r2;i++)
		{
			res_r2.add(i+".db");
		}
		Collections.sort(res_r2);
		String pagesTraceString_r2 = "r2{ " + String.join(" ",res_r2);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_r2+")", fileManagerTrace3.contains(pagesTraceString_r2));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test142TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"vap","zg2","y6","c6w90","muf","qz2oc","o17","c2nq7","g5h","ni17u"};
		DBApp.createTable("pq", cols0);
		for(int i=0;i<32;i++)
		{
			String [] record_pq = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_pq[j] = genRandString();
			}
			DBApp.insert("pq", record_pq);
		}
		int pagesCount_pq = (int)Math.ceil(32.0/DBApp.dataPageSize);
		ArrayList<String> res_pq = new ArrayList<>();
		for(int i=0;i<pagesCount_pq;i++)
		{
			res_pq.add(i+".db");
		}
		Collections.sort(res_pq);
		String pagesTraceString_pq = "pq{ " + String.join(" ",res_pq);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_pq+")", fileManagerTrace0.contains(pagesTraceString_pq));
		//--------------------------------------------------------------------------
		String[] cols1 = {"vx4","z3y6f"};
		DBApp.createTable("tame", cols1);
		for(int i=0;i<29;i++)
		{
			String [] record_tame = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_tame[j] = genRandString();
			}
			DBApp.insert("tame", record_tame);
		}
		int pagesCount_tame = (int)Math.ceil(29.0/DBApp.dataPageSize);
		ArrayList<String> res_tame = new ArrayList<>();
		for(int i=0;i<pagesCount_tame;i++)
		{
			res_tame.add(i+".db");
		}
		Collections.sort(res_tame);
		String pagesTraceString_tame = "tame{ " + String.join(" ",res_tame);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_tame+")", fileManagerTrace1.contains(pagesTraceString_tame));
		//--------------------------------------------------------------------------
		String[] cols2 = {"d89","l5c8","t09","pe","k1m9"};
		DBApp.createTable("nsm", cols2);
		for(int i=0;i<100;i++)
		{
			String [] record_nsm = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_nsm[j] = genRandString();
			}
			DBApp.insert("nsm", record_nsm);
		}
		int pagesCount_nsm = (int)Math.ceil(100.0/DBApp.dataPageSize);
		ArrayList<String> res_nsm = new ArrayList<>();
		for(int i=0;i<pagesCount_nsm;i++)
		{
			res_nsm.add(i+".db");
		}
		Collections.sort(res_nsm);
		String pagesTraceString_nsm = "nsm{ " + String.join(" ",res_nsm);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_nsm+")", fileManagerTrace2.contains(pagesTraceString_nsm));
		//--------------------------------------------------------------------------
		String[] cols3 = {"nb","cfkv","z3","h724g","r89","t1nk4"};
		DBApp.createTable("d3", cols3);
		for(int i=0;i<5;i++)
		{
			String [] record_d3 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_d3[j] = genRandString();
			}
			DBApp.insert("d3", record_d3);
		}
		int pagesCount_d3 = (int)Math.ceil(5.0/DBApp.dataPageSize);
		ArrayList<String> res_d3 = new ArrayList<>();
		for(int i=0;i<pagesCount_d3;i++)
		{
			res_d3.add(i+".db");
		}
		Collections.sort(res_d3);
		String pagesTraceString_d3 = "d3{ " + String.join(" ",res_d3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_d3+")", fileManagerTrace3.contains(pagesTraceString_d3));
		//--------------------------------------------------------------------------
		String[] cols4 = {"dtn","t8g"};
		DBApp.createTable("n0", cols4);
		for(int i=0;i<1;i++)
		{
			String [] record_n0 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_n0[j] = genRandString();
			}
			DBApp.insert("n0", record_n0);
		}
		int pagesCount_n0 = (int)Math.ceil(1.0/DBApp.dataPageSize);
		ArrayList<String> res_n0 = new ArrayList<>();
		for(int i=0;i<pagesCount_n0;i++)
		{
			res_n0.add(i+".db");
		}
		Collections.sort(res_n0);
		String pagesTraceString_n0 = "n0{ " + String.join(" ",res_n0);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_n0+")", fileManagerTrace4.contains(pagesTraceString_n0));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test143TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"kf54","l3l","wk5","z92","us","vkn"};
		DBApp.createTable("idr43", cols0);
		for(int i=0;i<25;i++)
		{
			String [] record_idr43 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_idr43[j] = genRandString();
			}
			DBApp.insert("idr43", record_idr43);
		}
		int pagesCount_idr43 = (int)Math.ceil(25.0/DBApp.dataPageSize);
		ArrayList<String> res_idr43 = new ArrayList<>();
		for(int i=0;i<pagesCount_idr43;i++)
		{
			res_idr43.add(i+".db");
		}
		Collections.sort(res_idr43);
		String pagesTraceString_idr43 = "idr43{ " + String.join(" ",res_idr43);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_idr43+")", fileManagerTrace0.contains(pagesTraceString_idr43));
		//--------------------------------------------------------------------------
		String[] cols1 = {"rc","y6","n09","w2y","tv","a34","ci7","ldi","w5lp","e7v"};
		DBApp.createTable("l7h", cols1);
		for(int i=0;i<37;i++)
		{
			String [] record_l7h = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_l7h[j] = genRandString();
			}
			DBApp.insert("l7h", record_l7h);
		}
		int pagesCount_l7h = (int)Math.ceil(37.0/DBApp.dataPageSize);
		ArrayList<String> res_l7h = new ArrayList<>();
		for(int i=0;i<pagesCount_l7h;i++)
		{
			res_l7h.add(i+".db");
		}
		Collections.sort(res_l7h);
		String pagesTraceString_l7h = "l7h{ " + String.join(" ",res_l7h);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_l7h+")", fileManagerTrace1.contains(pagesTraceString_l7h));
		//--------------------------------------------------------------------------
		String[] cols2 = {"sfu","dv7k","a2","b9y","tgsd","l79","n8o"};
		DBApp.createTable("xgp", cols2);
		for(int i=0;i<92;i++)
		{
			String [] record_xgp = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_xgp[j] = genRandString();
			}
			DBApp.insert("xgp", record_xgp);
		}
		int pagesCount_xgp = (int)Math.ceil(92.0/DBApp.dataPageSize);
		ArrayList<String> res_xgp = new ArrayList<>();
		for(int i=0;i<pagesCount_xgp;i++)
		{
			res_xgp.add(i+".db");
		}
		Collections.sort(res_xgp);
		String pagesTraceString_xgp = "xgp{ " + String.join(" ",res_xgp);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_xgp+")", fileManagerTrace2.contains(pagesTraceString_xgp));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test144TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"ib","x7","q75","a9s5"};
		DBApp.createTable("sb3", cols0);
		for(int i=0;i<42;i++)
		{
			String [] record_sb3 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_sb3[j] = genRandString();
			}
			DBApp.insert("sb3", record_sb3);
		}
		int pagesCount_sb3 = (int)Math.ceil(42.0/DBApp.dataPageSize);
		ArrayList<String> res_sb3 = new ArrayList<>();
		for(int i=0;i<pagesCount_sb3;i++)
		{
			res_sb3.add(i+".db");
		}
		Collections.sort(res_sb3);
		String pagesTraceString_sb3 = "sb3{ " + String.join(" ",res_sb3);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_sb3+")", fileManagerTrace0.contains(pagesTraceString_sb3));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ab8"};
		DBApp.createTable("p1", cols1);
		for(int i=0;i<77;i++)
		{
			String [] record_p1 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_p1[j] = genRandString();
			}
			DBApp.insert("p1", record_p1);
		}
		int pagesCount_p1 = (int)Math.ceil(77.0/DBApp.dataPageSize);
		ArrayList<String> res_p1 = new ArrayList<>();
		for(int i=0;i<pagesCount_p1;i++)
		{
			res_p1.add(i+".db");
		}
		Collections.sort(res_p1);
		String pagesTraceString_p1 = "p1{ " + String.join(" ",res_p1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_p1+")", fileManagerTrace1.contains(pagesTraceString_p1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"u3109","cd"};
		DBApp.createTable("n5", cols2);
		for(int i=0;i<42;i++)
		{
			String [] record_n5 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_n5[j] = genRandString();
			}
			DBApp.insert("n5", record_n5);
		}
		int pagesCount_n5 = (int)Math.ceil(42.0/DBApp.dataPageSize);
		ArrayList<String> res_n5 = new ArrayList<>();
		for(int i=0;i<pagesCount_n5;i++)
		{
			res_n5.add(i+".db");
		}
		Collections.sort(res_n5);
		String pagesTraceString_n5 = "n5{ " + String.join(" ",res_n5);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_n5+")", fileManagerTrace2.contains(pagesTraceString_n5));
		//--------------------------------------------------------------------------
		String[] cols3 = {"s33","f96","nj","ua04"};
		DBApp.createTable("doa", cols3);
		for(int i=0;i<24;i++)
		{
			String [] record_doa = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_doa[j] = genRandString();
			}
			DBApp.insert("doa", record_doa);
		}
		int pagesCount_doa = (int)Math.ceil(24.0/DBApp.dataPageSize);
		ArrayList<String> res_doa = new ArrayList<>();
		for(int i=0;i<pagesCount_doa;i++)
		{
			res_doa.add(i+".db");
		}
		Collections.sort(res_doa);
		String pagesTraceString_doa = "doa{ " + String.join(" ",res_doa);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_doa+")", fileManagerTrace3.contains(pagesTraceString_doa));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test145TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"s21","nr","g1","xqq","fsl0","e70h","rgon"};
		DBApp.createTable("gbx0m", cols0);
		for(int i=0;i<40;i++)
		{
			String [] record_gbx0m = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_gbx0m[j] = genRandString();
			}
			DBApp.insert("gbx0m", record_gbx0m);
		}
		int pagesCount_gbx0m = (int)Math.ceil(40.0/DBApp.dataPageSize);
		ArrayList<String> res_gbx0m = new ArrayList<>();
		for(int i=0;i<pagesCount_gbx0m;i++)
		{
			res_gbx0m.add(i+".db");
		}
		Collections.sort(res_gbx0m);
		String pagesTraceString_gbx0m = "gbx0m{ " + String.join(" ",res_gbx0m);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_gbx0m+")", fileManagerTrace0.contains(pagesTraceString_gbx0m));
		//--------------------------------------------------------------------------
		String[] cols1 = {"x3sw","je","od","zwy4","rmtx","x0q","sd","d0","d55k","b5"};
		DBApp.createTable("l8", cols1);
		for(int i=0;i<99;i++)
		{
			String [] record_l8 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_l8[j] = genRandString();
			}
			DBApp.insert("l8", record_l8);
		}
		int pagesCount_l8 = (int)Math.ceil(99.0/DBApp.dataPageSize);
		ArrayList<String> res_l8 = new ArrayList<>();
		for(int i=0;i<pagesCount_l8;i++)
		{
			res_l8.add(i+".db");
		}
		Collections.sort(res_l8);
		String pagesTraceString_l8 = "l8{ " + String.join(" ",res_l8);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_l8+")", fileManagerTrace1.contains(pagesTraceString_l8));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test146TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"g198v","amd9","ddj","n7s45"};
		DBApp.createTable("pr", cols0);
		for(int i=0;i<3;i++)
		{
			String [] record_pr = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_pr[j] = genRandString();
			}
			DBApp.insert("pr", record_pr);
		}
		int pagesCount_pr = (int)Math.ceil(3.0/DBApp.dataPageSize);
		ArrayList<String> res_pr = new ArrayList<>();
		for(int i=0;i<pagesCount_pr;i++)
		{
			res_pr.add(i+".db");
		}
		Collections.sort(res_pr);
		String pagesTraceString_pr = "pr{ " + String.join(" ",res_pr);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_pr+")", fileManagerTrace0.contains(pagesTraceString_pr));
		//--------------------------------------------------------------------------
		String[] cols1 = {"c8b","knft"};
		DBApp.createTable("m31", cols1);
		for(int i=0;i<60;i++)
		{
			String [] record_m31 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_m31[j] = genRandString();
			}
			DBApp.insert("m31", record_m31);
		}
		int pagesCount_m31 = (int)Math.ceil(60.0/DBApp.dataPageSize);
		ArrayList<String> res_m31 = new ArrayList<>();
		for(int i=0;i<pagesCount_m31;i++)
		{
			res_m31.add(i+".db");
		}
		Collections.sort(res_m31);
		String pagesTraceString_m31 = "m31{ " + String.join(" ",res_m31);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_m31+")", fileManagerTrace1.contains(pagesTraceString_m31));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test147TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"by7","p66"};
		DBApp.createTable("jh750", cols0);
		for(int i=0;i<66;i++)
		{
			String [] record_jh750 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_jh750[j] = genRandString();
			}
			DBApp.insert("jh750", record_jh750);
		}
		int pagesCount_jh750 = (int)Math.ceil(66.0/DBApp.dataPageSize);
		ArrayList<String> res_jh750 = new ArrayList<>();
		for(int i=0;i<pagesCount_jh750;i++)
		{
			res_jh750.add(i+".db");
		}
		Collections.sort(res_jh750);
		String pagesTraceString_jh750 = "jh750{ " + String.join(" ",res_jh750);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_jh750+")", fileManagerTrace0.contains(pagesTraceString_jh750));
		//--------------------------------------------------------------------------
		String[] cols1 = {"q3","a62","onr"};
		DBApp.createTable("h5s0", cols1);
		for(int i=0;i<16;i++)
		{
			String [] record_h5s0 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_h5s0[j] = genRandString();
			}
			DBApp.insert("h5s0", record_h5s0);
		}
		int pagesCount_h5s0 = (int)Math.ceil(16.0/DBApp.dataPageSize);
		ArrayList<String> res_h5s0 = new ArrayList<>();
		for(int i=0;i<pagesCount_h5s0;i++)
		{
			res_h5s0.add(i+".db");
		}
		Collections.sort(res_h5s0);
		String pagesTraceString_h5s0 = "h5s0{ " + String.join(" ",res_h5s0);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_h5s0+")", fileManagerTrace1.contains(pagesTraceString_h5s0));
		//--------------------------------------------------------------------------
		String[] cols2 = {"n7k6t","gd3y","c490o","uzs2","d79a1","q4","grif1"};
		DBApp.createTable("d9l6g", cols2);
		for(int i=0;i<8;i++)
		{
			String [] record_d9l6g = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_d9l6g[j] = genRandString();
			}
			DBApp.insert("d9l6g", record_d9l6g);
		}
		int pagesCount_d9l6g = (int)Math.ceil(8.0/DBApp.dataPageSize);
		ArrayList<String> res_d9l6g = new ArrayList<>();
		for(int i=0;i<pagesCount_d9l6g;i++)
		{
			res_d9l6g.add(i+".db");
		}
		Collections.sort(res_d9l6g);
		String pagesTraceString_d9l6g = "d9l6g{ " + String.join(" ",res_d9l6g);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_d9l6g+")", fileManagerTrace2.contains(pagesTraceString_d9l6g));
		//--------------------------------------------------------------------------
		String[] cols3 = {"p0r9w","fd","uki","j36"};
		DBApp.createTable("mms", cols3);
		for(int i=0;i<33;i++)
		{
			String [] record_mms = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_mms[j] = genRandString();
			}
			DBApp.insert("mms", record_mms);
		}
		int pagesCount_mms = (int)Math.ceil(33.0/DBApp.dataPageSize);
		ArrayList<String> res_mms = new ArrayList<>();
		for(int i=0;i<pagesCount_mms;i++)
		{
			res_mms.add(i+".db");
		}
		Collections.sort(res_mms);
		String pagesTraceString_mms = "mms{ " + String.join(" ",res_mms);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_mms+")", fileManagerTrace3.contains(pagesTraceString_mms));
		//--------------------------------------------------------------------------
		String[] cols4 = {"q39","dl27o","e68","a6xrf","xu9","om41"};
		DBApp.createTable("f2a", cols4);
		for(int i=0;i<91;i++)
		{
			String [] record_f2a = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_f2a[j] = genRandString();
			}
			DBApp.insert("f2a", record_f2a);
		}
		int pagesCount_f2a = (int)Math.ceil(91.0/DBApp.dataPageSize);
		ArrayList<String> res_f2a = new ArrayList<>();
		for(int i=0;i<pagesCount_f2a;i++)
		{
			res_f2a.add(i+".db");
		}
		Collections.sort(res_f2a);
		String pagesTraceString_f2a = "f2a{ " + String.join(" ",res_f2a);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_f2a+")", fileManagerTrace4.contains(pagesTraceString_f2a));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test148TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"os0","bk02","lezio","s784e","hi3sc","gk0"};
		DBApp.createTable("ak", cols0);
		for(int i=0;i<45;i++)
		{
			String [] record_ak = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ak[j] = genRandString();
			}
			DBApp.insert("ak", record_ak);
		}
		int pagesCount_ak = (int)Math.ceil(45.0/DBApp.dataPageSize);
		ArrayList<String> res_ak = new ArrayList<>();
		for(int i=0;i<pagesCount_ak;i++)
		{
			res_ak.add(i+".db");
		}
		Collections.sort(res_ak);
		String pagesTraceString_ak = "ak{ " + String.join(" ",res_ak);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ak+")", fileManagerTrace0.contains(pagesTraceString_ak));
		//--------------------------------------------------------------------------
		String[] cols1 = {"r86","o35","vx6n","u9i","sf37r","b3jzn","d7cnm","st7"};
		DBApp.createTable("r57", cols1);
		for(int i=0;i<79;i++)
		{
			String [] record_r57 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_r57[j] = genRandString();
			}
			DBApp.insert("r57", record_r57);
		}
		int pagesCount_r57 = (int)Math.ceil(79.0/DBApp.dataPageSize);
		ArrayList<String> res_r57 = new ArrayList<>();
		for(int i=0;i<pagesCount_r57;i++)
		{
			res_r57.add(i+".db");
		}
		Collections.sort(res_r57);
		String pagesTraceString_r57 = "r57{ " + String.join(" ",res_r57);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_r57+")", fileManagerTrace1.contains(pagesTraceString_r57));
		//--------------------------------------------------------------------------
		String[] cols2 = {"nfl","a7m","vai","c665s","xs31"};
		DBApp.createTable("d5", cols2);
		for(int i=0;i<73;i++)
		{
			String [] record_d5 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_d5[j] = genRandString();
			}
			DBApp.insert("d5", record_d5);
		}
		int pagesCount_d5 = (int)Math.ceil(73.0/DBApp.dataPageSize);
		ArrayList<String> res_d5 = new ArrayList<>();
		for(int i=0;i<pagesCount_d5;i++)
		{
			res_d5.add(i+".db");
		}
		Collections.sort(res_d5);
		String pagesTraceString_d5 = "d5{ " + String.join(" ",res_d5);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_d5+")", fileManagerTrace2.contains(pagesTraceString_d5));
		//--------------------------------------------------------------------------
		String[] cols3 = {"bku","kg","pj6es","w8n","phlkv","a6qs6","m67","ax1lu"};
		DBApp.createTable("rv", cols3);
		for(int i=0;i<66;i++)
		{
			String [] record_rv = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_rv[j] = genRandString();
			}
			DBApp.insert("rv", record_rv);
		}
		int pagesCount_rv = (int)Math.ceil(66.0/DBApp.dataPageSize);
		ArrayList<String> res_rv = new ArrayList<>();
		for(int i=0;i<pagesCount_rv;i++)
		{
			res_rv.add(i+".db");
		}
		Collections.sort(res_rv);
		String pagesTraceString_rv = "rv{ " + String.join(" ",res_rv);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_rv+")", fileManagerTrace3.contains(pagesTraceString_rv));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test149TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"y90i","as2zm","fb","y58","gwx","j6","p0y"};
		DBApp.createTable("o2q", cols0);
		for(int i=0;i<54;i++)
		{
			String [] record_o2q = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_o2q[j] = genRandString();
			}
			DBApp.insert("o2q", record_o2q);
		}
		int pagesCount_o2q = (int)Math.ceil(54.0/DBApp.dataPageSize);
		ArrayList<String> res_o2q = new ArrayList<>();
		for(int i=0;i<pagesCount_o2q;i++)
		{
			res_o2q.add(i+".db");
		}
		Collections.sort(res_o2q);
		String pagesTraceString_o2q = "o2q{ " + String.join(" ",res_o2q);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_o2q+")", fileManagerTrace0.contains(pagesTraceString_o2q));
		//--------------------------------------------------------------------------
		String[] cols1 = {"m0620","d3b","eq6","vx9","d138t","u9bb","aq22z","wqqim","s65","fc9"};
		DBApp.createTable("m7p1", cols1);
		for(int i=0;i<91;i++)
		{
			String [] record_m7p1 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_m7p1[j] = genRandString();
			}
			DBApp.insert("m7p1", record_m7p1);
		}
		int pagesCount_m7p1 = (int)Math.ceil(91.0/DBApp.dataPageSize);
		ArrayList<String> res_m7p1 = new ArrayList<>();
		for(int i=0;i<pagesCount_m7p1;i++)
		{
			res_m7p1.add(i+".db");
		}
		Collections.sort(res_m7p1);
		String pagesTraceString_m7p1 = "m7p1{ " + String.join(" ",res_m7p1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_m7p1+")", fileManagerTrace1.contains(pagesTraceString_m7p1));
		//--------------------------------------------------------------------------
		String[] cols2 = {"k58","aj3","g6","df2g","pj0","r28","ywp","tog"};
		DBApp.createTable("kds16", cols2);
		for(int i=0;i<57;i++)
		{
			String [] record_kds16 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_kds16[j] = genRandString();
			}
			DBApp.insert("kds16", record_kds16);
		}
		int pagesCount_kds16 = (int)Math.ceil(57.0/DBApp.dataPageSize);
		ArrayList<String> res_kds16 = new ArrayList<>();
		for(int i=0;i<pagesCount_kds16;i++)
		{
			res_kds16.add(i+".db");
		}
		Collections.sort(res_kds16);
		String pagesTraceString_kds16 = "kds16{ " + String.join(" ",res_kds16);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_kds16+")", fileManagerTrace2.contains(pagesTraceString_kds16));
		//--------------------------------------------------------------------------
		String[] cols3 = {"h5o0","owit","bj2n","v4","b9","k3bl8","n6","ax"};
		DBApp.createTable("lr7s", cols3);
		for(int i=0;i<98;i++)
		{
			String [] record_lr7s = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_lr7s[j] = genRandString();
			}
			DBApp.insert("lr7s", record_lr7s);
		}
		int pagesCount_lr7s = (int)Math.ceil(98.0/DBApp.dataPageSize);
		ArrayList<String> res_lr7s = new ArrayList<>();
		for(int i=0;i<pagesCount_lr7s;i++)
		{
			res_lr7s.add(i+".db");
		}
		Collections.sort(res_lr7s);
		String pagesTraceString_lr7s = "lr7s{ " + String.join(" ",res_lr7s);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_lr7s+")", fileManagerTrace3.contains(pagesTraceString_lr7s));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test150TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"nu2t","aq2","mli7","yy7m","yl2","vx","o8p35","vl2","bf5"};
		DBApp.createTable("pr945", cols0);
		for(int i=0;i<75;i++)
		{
			String [] record_pr945 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_pr945[j] = genRandString();
			}
			DBApp.insert("pr945", record_pr945);
		}
		int pagesCount_pr945 = (int)Math.ceil(75.0/DBApp.dataPageSize);
		ArrayList<String> res_pr945 = new ArrayList<>();
		for(int i=0;i<pagesCount_pr945;i++)
		{
			res_pr945.add(i+".db");
		}
		Collections.sort(res_pr945);
		String pagesTraceString_pr945 = "pr945{ " + String.join(" ",res_pr945);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_pr945+")", fileManagerTrace0.contains(pagesTraceString_pr945));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test151TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"h8","gtq","e16","f0r","rp634"};
		DBApp.createTable("p3", cols0);
		for(int i=0;i<9;i++)
		{
			String [] record_p3 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_p3[j] = genRandString();
			}
			DBApp.insert("p3", record_p3);
		}
		int pagesCount_p3 = (int)Math.ceil(9.0/DBApp.dataPageSize);
		ArrayList<String> res_p3 = new ArrayList<>();
		for(int i=0;i<pagesCount_p3;i++)
		{
			res_p3.add(i+".db");
		}
		Collections.sort(res_p3);
		String pagesTraceString_p3 = "p3{ " + String.join(" ",res_p3);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_p3+")", fileManagerTrace0.contains(pagesTraceString_p3));
		//--------------------------------------------------------------------------
		String[] cols1 = {"xu4","a5h","u8","gzwd","b972"};
		DBApp.createTable("hbtt", cols1);
		for(int i=0;i<67;i++)
		{
			String [] record_hbtt = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_hbtt[j] = genRandString();
			}
			DBApp.insert("hbtt", record_hbtt);
		}
		int pagesCount_hbtt = (int)Math.ceil(67.0/DBApp.dataPageSize);
		ArrayList<String> res_hbtt = new ArrayList<>();
		for(int i=0;i<pagesCount_hbtt;i++)
		{
			res_hbtt.add(i+".db");
		}
		Collections.sort(res_hbtt);
		String pagesTraceString_hbtt = "hbtt{ " + String.join(" ",res_hbtt);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_hbtt+")", fileManagerTrace1.contains(pagesTraceString_hbtt));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test152TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"i9ocp","i5","r9l","yz"};
		DBApp.createTable("g5", cols0);
		for(int i=0;i<2;i++)
		{
			String [] record_g5 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_g5[j] = genRandString();
			}
			DBApp.insert("g5", record_g5);
		}
		int pagesCount_g5 = (int)Math.ceil(2.0/DBApp.dataPageSize);
		ArrayList<String> res_g5 = new ArrayList<>();
		for(int i=0;i<pagesCount_g5;i++)
		{
			res_g5.add(i+".db");
		}
		Collections.sort(res_g5);
		String pagesTraceString_g5 = "g5{ " + String.join(" ",res_g5);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_g5+")", fileManagerTrace0.contains(pagesTraceString_g5));
		//--------------------------------------------------------------------------
		String[] cols1 = {"pl0","i6e9","uu4v9"};
		DBApp.createTable("t667", cols1);
		for(int i=0;i<83;i++)
		{
			String [] record_t667 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_t667[j] = genRandString();
			}
			DBApp.insert("t667", record_t667);
		}
		int pagesCount_t667 = (int)Math.ceil(83.0/DBApp.dataPageSize);
		ArrayList<String> res_t667 = new ArrayList<>();
		for(int i=0;i<pagesCount_t667;i++)
		{
			res_t667.add(i+".db");
		}
		Collections.sort(res_t667);
		String pagesTraceString_t667 = "t667{ " + String.join(" ",res_t667);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_t667+")", fileManagerTrace1.contains(pagesTraceString_t667));
		//--------------------------------------------------------------------------
		String[] cols2 = {"yp3","mx2s"};
		DBApp.createTable("yn7", cols2);
		for(int i=0;i<1;i++)
		{
			String [] record_yn7 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_yn7[j] = genRandString();
			}
			DBApp.insert("yn7", record_yn7);
		}
		int pagesCount_yn7 = (int)Math.ceil(1.0/DBApp.dataPageSize);
		ArrayList<String> res_yn7 = new ArrayList<>();
		for(int i=0;i<pagesCount_yn7;i++)
		{
			res_yn7.add(i+".db");
		}
		Collections.sort(res_yn7);
		String pagesTraceString_yn7 = "yn7{ " + String.join(" ",res_yn7);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_yn7+")", fileManagerTrace2.contains(pagesTraceString_yn7));
		//--------------------------------------------------------------------------
		String[] cols3 = {"nm","h4u","y2730","ptyhp","f6gt","xz","e19","cct","xvs"};
		DBApp.createTable("jt1tu", cols3);
		for(int i=0;i<48;i++)
		{
			String [] record_jt1tu = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_jt1tu[j] = genRandString();
			}
			DBApp.insert("jt1tu", record_jt1tu);
		}
		int pagesCount_jt1tu = (int)Math.ceil(48.0/DBApp.dataPageSize);
		ArrayList<String> res_jt1tu = new ArrayList<>();
		for(int i=0;i<pagesCount_jt1tu;i++)
		{
			res_jt1tu.add(i+".db");
		}
		Collections.sort(res_jt1tu);
		String pagesTraceString_jt1tu = "jt1tu{ " + String.join(" ",res_jt1tu);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_jt1tu+")", fileManagerTrace3.contains(pagesTraceString_jt1tu));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test153TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"dzy","g1ks","hh9","o356n","zyg","j454","w06","cr","zig"};
		DBApp.createTable("k5q", cols0);
		for(int i=0;i<38;i++)
		{
			String [] record_k5q = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_k5q[j] = genRandString();
			}
			DBApp.insert("k5q", record_k5q);
		}
		int pagesCount_k5q = (int)Math.ceil(38.0/DBApp.dataPageSize);
		ArrayList<String> res_k5q = new ArrayList<>();
		for(int i=0;i<pagesCount_k5q;i++)
		{
			res_k5q.add(i+".db");
		}
		Collections.sort(res_k5q);
		String pagesTraceString_k5q = "k5q{ " + String.join(" ",res_k5q);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_k5q+")", fileManagerTrace0.contains(pagesTraceString_k5q));
		//--------------------------------------------------------------------------
		String[] cols1 = {"d111i","ge4","ni9","o20k","qz5h","jakx","bjr","arx"};
		DBApp.createTable("nbj6m", cols1);
		for(int i=0;i<74;i++)
		{
			String [] record_nbj6m = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_nbj6m[j] = genRandString();
			}
			DBApp.insert("nbj6m", record_nbj6m);
		}
		int pagesCount_nbj6m = (int)Math.ceil(74.0/DBApp.dataPageSize);
		ArrayList<String> res_nbj6m = new ArrayList<>();
		for(int i=0;i<pagesCount_nbj6m;i++)
		{
			res_nbj6m.add(i+".db");
		}
		Collections.sort(res_nbj6m);
		String pagesTraceString_nbj6m = "nbj6m{ " + String.join(" ",res_nbj6m);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_nbj6m+")", fileManagerTrace1.contains(pagesTraceString_nbj6m));
		//--------------------------------------------------------------------------
		String[] cols2 = {"c87","q2il2","s833"};
		DBApp.createTable("r25", cols2);
		for(int i=0;i<96;i++)
		{
			String [] record_r25 = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_r25[j] = genRandString();
			}
			DBApp.insert("r25", record_r25);
		}
		int pagesCount_r25 = (int)Math.ceil(96.0/DBApp.dataPageSize);
		ArrayList<String> res_r25 = new ArrayList<>();
		for(int i=0;i<pagesCount_r25;i++)
		{
			res_r25.add(i+".db");
		}
		Collections.sort(res_r25);
		String pagesTraceString_r25 = "r25{ " + String.join(" ",res_r25);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_r25+")", fileManagerTrace2.contains(pagesTraceString_r25));
		//--------------------------------------------------------------------------
		String[] cols3 = {"kg","kgy","lej4g","yn","b0x4t"};
		DBApp.createTable("qq2", cols3);
		for(int i=0;i<99;i++)
		{
			String [] record_qq2 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_qq2[j] = genRandString();
			}
			DBApp.insert("qq2", record_qq2);
		}
		int pagesCount_qq2 = (int)Math.ceil(99.0/DBApp.dataPageSize);
		ArrayList<String> res_qq2 = new ArrayList<>();
		for(int i=0;i<pagesCount_qq2;i++)
		{
			res_qq2.add(i+".db");
		}
		Collections.sort(res_qq2);
		String pagesTraceString_qq2 = "qq2{ " + String.join(" ",res_qq2);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_qq2+")", fileManagerTrace3.contains(pagesTraceString_qq2));
		//--------------------------------------------------------------------------
		String[] cols4 = {"t67","ec","jjg","r2","ux7","z8q","v9","g3","ik71"};
		DBApp.createTable("kvk", cols4);
		for(int i=0;i<95;i++)
		{
			String [] record_kvk = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_kvk[j] = genRandString();
			}
			DBApp.insert("kvk", record_kvk);
		}
		int pagesCount_kvk = (int)Math.ceil(95.0/DBApp.dataPageSize);
		ArrayList<String> res_kvk = new ArrayList<>();
		for(int i=0;i<pagesCount_kvk;i++)
		{
			res_kvk.add(i+".db");
		}
		Collections.sort(res_kvk);
		String pagesTraceString_kvk = "kvk{ " + String.join(" ",res_kvk);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_kvk+")", fileManagerTrace4.contains(pagesTraceString_kvk));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test154TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"kv","ag2","ms52","aln26","n0y","p0","n2t","w038","g9","q3964"};
		DBApp.createTable("f3", cols0);
		for(int i=0;i<74;i++)
		{
			String [] record_f3 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_f3[j] = genRandString();
			}
			DBApp.insert("f3", record_f3);
		}
		int pagesCount_f3 = (int)Math.ceil(74.0/DBApp.dataPageSize);
		ArrayList<String> res_f3 = new ArrayList<>();
		for(int i=0;i<pagesCount_f3;i++)
		{
			res_f3.add(i+".db");
		}
		Collections.sort(res_f3);
		String pagesTraceString_f3 = "f3{ " + String.join(" ",res_f3);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_f3+")", fileManagerTrace0.contains(pagesTraceString_f3));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ix0p","l5","imwnu","z01"};
		DBApp.createTable("ieb", cols1);
		for(int i=0;i<36;i++)
		{
			String [] record_ieb = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_ieb[j] = genRandString();
			}
			DBApp.insert("ieb", record_ieb);
		}
		int pagesCount_ieb = (int)Math.ceil(36.0/DBApp.dataPageSize);
		ArrayList<String> res_ieb = new ArrayList<>();
		for(int i=0;i<pagesCount_ieb;i++)
		{
			res_ieb.add(i+".db");
		}
		Collections.sort(res_ieb);
		String pagesTraceString_ieb = "ieb{ " + String.join(" ",res_ieb);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ieb+")", fileManagerTrace1.contains(pagesTraceString_ieb));
		//--------------------------------------------------------------------------
		String[] cols2 = {"v75","gni"};
		DBApp.createTable("liuv", cols2);
		for(int i=0;i<92;i++)
		{
			String [] record_liuv = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_liuv[j] = genRandString();
			}
			DBApp.insert("liuv", record_liuv);
		}
		int pagesCount_liuv = (int)Math.ceil(92.0/DBApp.dataPageSize);
		ArrayList<String> res_liuv = new ArrayList<>();
		for(int i=0;i<pagesCount_liuv;i++)
		{
			res_liuv.add(i+".db");
		}
		Collections.sort(res_liuv);
		String pagesTraceString_liuv = "liuv{ " + String.join(" ",res_liuv);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_liuv+")", fileManagerTrace2.contains(pagesTraceString_liuv));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test155TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"y9v","wl","dbg","q5q4","sw","yh3"};
		DBApp.createTable("v3", cols0);
		for(int i=0;i<56;i++)
		{
			String [] record_v3 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_v3[j] = genRandString();
			}
			DBApp.insert("v3", record_v3);
		}
		int pagesCount_v3 = (int)Math.ceil(56.0/DBApp.dataPageSize);
		ArrayList<String> res_v3 = new ArrayList<>();
		for(int i=0;i<pagesCount_v3;i++)
		{
			res_v3.add(i+".db");
		}
		Collections.sort(res_v3);
		String pagesTraceString_v3 = "v3{ " + String.join(" ",res_v3);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_v3+")", fileManagerTrace0.contains(pagesTraceString_v3));
		//--------------------------------------------------------------------------
		String[] cols1 = {"ic81","nze"};
		DBApp.createTable("ac", cols1);
		for(int i=0;i<55;i++)
		{
			String [] record_ac = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_ac[j] = genRandString();
			}
			DBApp.insert("ac", record_ac);
		}
		int pagesCount_ac = (int)Math.ceil(55.0/DBApp.dataPageSize);
		ArrayList<String> res_ac = new ArrayList<>();
		for(int i=0;i<pagesCount_ac;i++)
		{
			res_ac.add(i+".db");
		}
		Collections.sort(res_ac);
		String pagesTraceString_ac = "ac{ " + String.join(" ",res_ac);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ac+")", fileManagerTrace1.contains(pagesTraceString_ac));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test156TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a89","zg7"};
		DBApp.createTable("qvz", cols0);
		for(int i=0;i<63;i++)
		{
			String [] record_qvz = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_qvz[j] = genRandString();
			}
			DBApp.insert("qvz", record_qvz);
		}
		int pagesCount_qvz = (int)Math.ceil(63.0/DBApp.dataPageSize);
		ArrayList<String> res_qvz = new ArrayList<>();
		for(int i=0;i<pagesCount_qvz;i++)
		{
			res_qvz.add(i+".db");
		}
		Collections.sort(res_qvz);
		String pagesTraceString_qvz = "qvz{ " + String.join(" ",res_qvz);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_qvz+")", fileManagerTrace0.contains(pagesTraceString_qvz));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test157TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"qi"};
		DBApp.createTable("frh1", cols0);
		for(int i=0;i<99;i++)
		{
			String [] record_frh1 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_frh1[j] = genRandString();
			}
			DBApp.insert("frh1", record_frh1);
		}
		int pagesCount_frh1 = (int)Math.ceil(99.0/DBApp.dataPageSize);
		ArrayList<String> res_frh1 = new ArrayList<>();
		for(int i=0;i<pagesCount_frh1;i++)
		{
			res_frh1.add(i+".db");
		}
		Collections.sort(res_frh1);
		String pagesTraceString_frh1 = "frh1{ " + String.join(" ",res_frh1);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_frh1+")", fileManagerTrace0.contains(pagesTraceString_frh1));
		//--------------------------------------------------------------------------
		String[] cols1 = {"f5006"};
		DBApp.createTable("mzxpt", cols1);
		for(int i=0;i<41;i++)
		{
			String [] record_mzxpt = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_mzxpt[j] = genRandString();
			}
			DBApp.insert("mzxpt", record_mzxpt);
		}
		int pagesCount_mzxpt = (int)Math.ceil(41.0/DBApp.dataPageSize);
		ArrayList<String> res_mzxpt = new ArrayList<>();
		for(int i=0;i<pagesCount_mzxpt;i++)
		{
			res_mzxpt.add(i+".db");
		}
		Collections.sort(res_mzxpt);
		String pagesTraceString_mzxpt = "mzxpt{ " + String.join(" ",res_mzxpt);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_mzxpt+")", fileManagerTrace1.contains(pagesTraceString_mzxpt));
		//--------------------------------------------------------------------------
		String[] cols2 = {"aqh"};
		DBApp.createTable("kh", cols2);
		for(int i=0;i<71;i++)
		{
			String [] record_kh = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_kh[j] = genRandString();
			}
			DBApp.insert("kh", record_kh);
		}
		int pagesCount_kh = (int)Math.ceil(71.0/DBApp.dataPageSize);
		ArrayList<String> res_kh = new ArrayList<>();
		for(int i=0;i<pagesCount_kh;i++)
		{
			res_kh.add(i+".db");
		}
		Collections.sort(res_kh);
		String pagesTraceString_kh = "kh{ " + String.join(" ",res_kh);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_kh+")", fileManagerTrace2.contains(pagesTraceString_kh));
		//--------------------------------------------------------------------------
		String[] cols3 = {"tqw","k17","v46","jvb","q2","ef","p1g","icn10"};
		DBApp.createTable("z525a", cols3);
		for(int i=0;i<35;i++)
		{
			String [] record_z525a = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_z525a[j] = genRandString();
			}
			DBApp.insert("z525a", record_z525a);
		}
		int pagesCount_z525a = (int)Math.ceil(35.0/DBApp.dataPageSize);
		ArrayList<String> res_z525a = new ArrayList<>();
		for(int i=0;i<pagesCount_z525a;i++)
		{
			res_z525a.add(i+".db");
		}
		Collections.sort(res_z525a);
		String pagesTraceString_z525a = "z525a{ " + String.join(" ",res_z525a);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_z525a+")", fileManagerTrace3.contains(pagesTraceString_z525a));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test158TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"t4518","v99cr"};
		DBApp.createTable("ansg", cols0);
		for(int i=0;i<70;i++)
		{
			String [] record_ansg = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_ansg[j] = genRandString();
			}
			DBApp.insert("ansg", record_ansg);
		}
		int pagesCount_ansg = (int)Math.ceil(70.0/DBApp.dataPageSize);
		ArrayList<String> res_ansg = new ArrayList<>();
		for(int i=0;i<pagesCount_ansg;i++)
		{
			res_ansg.add(i+".db");
		}
		Collections.sort(res_ansg);
		String pagesTraceString_ansg = "ansg{ " + String.join(" ",res_ansg);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ansg+")", fileManagerTrace0.contains(pagesTraceString_ansg));
		//--------------------------------------------------------------------------
		String[] cols1 = {"kp7p0"};
		DBApp.createTable("od4", cols1);
		for(int i=0;i<72;i++)
		{
			String [] record_od4 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_od4[j] = genRandString();
			}
			DBApp.insert("od4", record_od4);
		}
		int pagesCount_od4 = (int)Math.ceil(72.0/DBApp.dataPageSize);
		ArrayList<String> res_od4 = new ArrayList<>();
		for(int i=0;i<pagesCount_od4;i++)
		{
			res_od4.add(i+".db");
		}
		Collections.sort(res_od4);
		String pagesTraceString_od4 = "od4{ " + String.join(" ",res_od4);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_od4+")", fileManagerTrace1.contains(pagesTraceString_od4));
		//--------------------------------------------------------------------------
		String[] cols2 = {"cnte","d38","apo2u","wm7","x8o","c42","c9ob"};
		DBApp.createTable("be", cols2);
		for(int i=0;i<29;i++)
		{
			String [] record_be = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_be[j] = genRandString();
			}
			DBApp.insert("be", record_be);
		}
		int pagesCount_be = (int)Math.ceil(29.0/DBApp.dataPageSize);
		ArrayList<String> res_be = new ArrayList<>();
		for(int i=0;i<pagesCount_be;i++)
		{
			res_be.add(i+".db");
		}
		Collections.sort(res_be);
		String pagesTraceString_be = "be{ " + String.join(" ",res_be);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_be+")", fileManagerTrace2.contains(pagesTraceString_be));
		//--------------------------------------------------------------------------
		String[] cols3 = {"ym055"};
		DBApp.createTable("i0", cols3);
		for(int i=0;i<2;i++)
		{
			String [] record_i0 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_i0[j] = genRandString();
			}
			DBApp.insert("i0", record_i0);
		}
		int pagesCount_i0 = (int)Math.ceil(2.0/DBApp.dataPageSize);
		ArrayList<String> res_i0 = new ArrayList<>();
		for(int i=0;i<pagesCount_i0;i++)
		{
			res_i0.add(i+".db");
		}
		Collections.sort(res_i0);
		String pagesTraceString_i0 = "i0{ " + String.join(" ",res_i0);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_i0+")", fileManagerTrace3.contains(pagesTraceString_i0));
		//--------------------------------------------------------------------------
		String[] cols4 = {"ofl","t2b4"};
		DBApp.createTable("fxy", cols4);
		for(int i=0;i<44;i++)
		{
			String [] record_fxy = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_fxy[j] = genRandString();
			}
			DBApp.insert("fxy", record_fxy);
		}
		int pagesCount_fxy = (int)Math.ceil(44.0/DBApp.dataPageSize);
		ArrayList<String> res_fxy = new ArrayList<>();
		for(int i=0;i<pagesCount_fxy;i++)
		{
			res_fxy.add(i+".db");
		}
		Collections.sort(res_fxy);
		String pagesTraceString_fxy = "fxy{ " + String.join(" ",res_fxy);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_fxy+")", fileManagerTrace4.contains(pagesTraceString_fxy));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test159TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"qlr"};
		DBApp.createTable("vmd", cols0);
		for(int i=0;i<88;i++)
		{
			String [] record_vmd = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_vmd[j] = genRandString();
			}
			DBApp.insert("vmd", record_vmd);
		}
		int pagesCount_vmd = (int)Math.ceil(88.0/DBApp.dataPageSize);
		ArrayList<String> res_vmd = new ArrayList<>();
		for(int i=0;i<pagesCount_vmd;i++)
		{
			res_vmd.add(i+".db");
		}
		Collections.sort(res_vmd);
		String pagesTraceString_vmd = "vmd{ " + String.join(" ",res_vmd);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_vmd+")", fileManagerTrace0.contains(pagesTraceString_vmd));
		//--------------------------------------------------------------------------
		String[] cols1 = {"k04","nxa1e","i35","k3d","pth","uuwp","k09j8","m4a"};
		DBApp.createTable("ds52", cols1);
		for(int i=0;i<85;i++)
		{
			String [] record_ds52 = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_ds52[j] = genRandString();
			}
			DBApp.insert("ds52", record_ds52);
		}
		int pagesCount_ds52 = (int)Math.ceil(85.0/DBApp.dataPageSize);
		ArrayList<String> res_ds52 = new ArrayList<>();
		for(int i=0;i<pagesCount_ds52;i++)
		{
			res_ds52.add(i+".db");
		}
		Collections.sort(res_ds52);
		String pagesTraceString_ds52 = "ds52{ " + String.join(" ",res_ds52);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_ds52+")", fileManagerTrace1.contains(pagesTraceString_ds52));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test160TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"fw","ajz","m6c","ft65"};
		DBApp.createTable("a24", cols0);
		for(int i=0;i<14;i++)
		{
			String [] record_a24 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_a24[j] = genRandString();
			}
			DBApp.insert("a24", record_a24);
		}
		int pagesCount_a24 = (int)Math.ceil(14.0/DBApp.dataPageSize);
		ArrayList<String> res_a24 = new ArrayList<>();
		for(int i=0;i<pagesCount_a24;i++)
		{
			res_a24.add(i+".db");
		}
		Collections.sort(res_a24);
		String pagesTraceString_a24 = "a24{ " + String.join(" ",res_a24);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_a24+")", fileManagerTrace0.contains(pagesTraceString_a24));
		//--------------------------------------------------------------------------
		String[] cols1 = {"d3","z5"};
		DBApp.createTable("dopc", cols1);
		for(int i=0;i<74;i++)
		{
			String [] record_dopc = new String[cols1.length];
			for(int j=0;j<cols1.length;j++)
			{
				record_dopc[j] = genRandString();
			}
			DBApp.insert("dopc", record_dopc);
		}
		int pagesCount_dopc = (int)Math.ceil(74.0/DBApp.dataPageSize);
		ArrayList<String> res_dopc = new ArrayList<>();
		for(int i=0;i<pagesCount_dopc;i++)
		{
			res_dopc.add(i+".db");
		}
		Collections.sort(res_dopc);
		String pagesTraceString_dopc = "dopc{ " + String.join(" ",res_dopc);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_dopc+")", fileManagerTrace1.contains(pagesTraceString_dopc));
		//--------------------------------------------------------------------------
		String[] cols2 = {"b6","l8a","h4g","yhx","cmy","xfx1"};
		DBApp.createTable("opj", cols2);
		for(int i=0;i<15;i++)
		{
			String [] record_opj = new String[cols2.length];
			for(int j=0;j<cols2.length;j++)
			{
				record_opj[j] = genRandString();
			}
			DBApp.insert("opj", record_opj);
		}
		int pagesCount_opj = (int)Math.ceil(15.0/DBApp.dataPageSize);
		ArrayList<String> res_opj = new ArrayList<>();
		for(int i=0;i<pagesCount_opj;i++)
		{
			res_opj.add(i+".db");
		}
		Collections.sort(res_opj);
		String pagesTraceString_opj = "opj{ " + String.join(" ",res_opj);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_opj+")", fileManagerTrace2.contains(pagesTraceString_opj));
		//--------------------------------------------------------------------------
		String[] cols3 = {"q257","b78","r8e99","h6","e70","qaejl","tu","f49","dd8","r7"};
		DBApp.createTable("v3", cols3);
		for(int i=0;i<67;i++)
		{
			String [] record_v3 = new String[cols3.length];
			for(int j=0;j<cols3.length;j++)
			{
				record_v3[j] = genRandString();
			}
			DBApp.insert("v3", record_v3);
		}
		int pagesCount_v3 = (int)Math.ceil(67.0/DBApp.dataPageSize);
		ArrayList<String> res_v3 = new ArrayList<>();
		for(int i=0;i<pagesCount_v3;i++)
		{
			res_v3.add(i+".db");
		}
		Collections.sort(res_v3);
		String pagesTraceString_v3 = "v3{ " + String.join(" ",res_v3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_v3+")", fileManagerTrace3.contains(pagesTraceString_v3));
		//--------------------------------------------------------------------------
		String[] cols4 = {"g0","o5","zh8m","pf","r2x","l1o"};
		DBApp.createTable("i81", cols4);
		for(int i=0;i<62;i++)
		{
			String [] record_i81 = new String[cols4.length];
			for(int j=0;j<cols4.length;j++)
			{
				record_i81[j] = genRandString();
			}
			DBApp.insert("i81", record_i81);
		}
		int pagesCount_i81 = (int)Math.ceil(62.0/DBApp.dataPageSize);
		ArrayList<String> res_i81 = new ArrayList<>();
		for(int i=0;i<pagesCount_i81;i++)
		{
			res_i81.add(i+".db");
		}
		Collections.sort(res_i81);
		String pagesTraceString_i81 = "i81{ " + String.join(" ",res_i81);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_i81+")", fileManagerTrace4.contains(pagesTraceString_i81));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void test161TableInsertionPageSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"o0v2y","r28","y537j"};
		DBApp.createTable("r427", cols0);
		for(int i=0;i<43;i++)
		{
			String [] record_r427 = new String[cols0.length];
			for(int j=0;j<cols0.length;j++)
			{
				record_r427[j] = genRandString();
			}
			DBApp.insert("r427", record_r427);
		}
		int pagesCount_r427 = (int)Math.ceil(43.0/DBApp.dataPageSize);
		ArrayList<String> res_r427 = new ArrayList<>();
		for(int i=0;i<pagesCount_r427;i++)
		{
			res_r427.add(i+".db");
		}
		Collections.sort(res_r427);
		String pagesTraceString_r427 = "r427{ " + String.join(" ",res_r427);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("Pages should be stored inside the folder of the table when records are inserted.("+pagesTraceString_r427+")", fileManagerTrace0.contains(pagesTraceString_r427));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(690);
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