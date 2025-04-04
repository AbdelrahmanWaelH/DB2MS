package com.dbapp;
import static org.junit.Assert.assertTrue;
import java.util.*;
import org.junit.Test;
public class DBAppTests01
{

	
	@Test(timeout = 10000)
	public void test0TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"p0ky8","ow6","zu2x","gm","c7p","av","caz","paph5","y5b","viye"};
		DBApp.createTable("d6ij", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("d6ij"));
		String[] cols1 = {"rv"};
		DBApp.createTable("v2", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("v2"));
		String[] cols2 = {"dmh1","c9","p27k6","n1l","acc8","kjo02","ct","z6bu","wkzf4"};
		DBApp.createTable("fmq", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("fmq"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test1TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"i0l30","zsv11"};
		DBApp.createTable("vtllt", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("vtllt"));
		String[] cols1 = {"tf","regok","v317u","ge72u","d5","jfz","k9x","wh","pz2"};
		DBApp.createTable("ev2n", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("ev2n"));
		String[] cols2 = {"h55","wao","rl82","lk6","e4","w88w"};
		DBApp.createTable("r88", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("r88"));
		String[] cols3 = {"je9","oj4","v57","w2u","eio5","bsl0"};
		DBApp.createTable("yz7", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("yz7"));
		String[] cols4 = {"b2","i4j","ng","t22","i77","f07zx","r60w","l6","in2ue","z75"};
		DBApp.createTable("vux6", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("vux6"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test2TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"j6uh","dj"};
		DBApp.createTable("p3d5", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("p3d5"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test3TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"pd","pbacv","oac","d3y","c40c"};
		DBApp.createTable("xx875", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("xx875"));
		String[] cols1 = {"lwz","f3m1e","i1k03","x1lk","ab","xir","id","b6w"};
		DBApp.createTable("n3n", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("n3n"));
		String[] cols2 = {"j4v9y","uy","k2","w88z","jzm71","dcjw7","w6","t9"};
		DBApp.createTable("w8xq", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("w8xq"));
		String[] cols3 = {"x8c0o"};
		DBApp.createTable("ta5nw", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("ta5nw"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test4TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"e8c","r8f","d21","u995v","z49"};
		DBApp.createTable("o1q", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("o1q"));
		String[] cols1 = {"mw5k","jxqw1","o0hl","t0k5"};
		DBApp.createTable("ck", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("ck"));
		String[] cols2 = {"rk56","eu34","lf9","ys","o7"};
		DBApp.createTable("g9", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("g9"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test5TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"de"};
		DBApp.createTable("wq0", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("wq0"));
		String[] cols1 = {"yox","h97m","p5","ul"};
		DBApp.createTable("d121", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("d121"));
		String[] cols2 = {"v0pu","a8b81","oz4"};
		DBApp.createTable("b6h", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("b6h"));
		String[] cols3 = {"d38","jm5","u77","ks"};
		DBApp.createTable("w69", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("w69"));
		String[] cols4 = {"i2t4"};
		DBApp.createTable("c7s0", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("c7s0"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test6TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"zw77h","hu0wz","f1x5","k63"};
		DBApp.createTable("g46k8", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("g46k8"));
		String[] cols1 = {"etm6l","e71"};
		DBApp.createTable("qd3k6", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("qd3k6"));
		String[] cols2 = {"ye","xq1i0"};
		DBApp.createTable("xoe", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("xoe"));
		String[] cols3 = {"w53"};
		DBApp.createTable("yh0d5", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("yh0d5"));
		String[] cols4 = {"y9o6","xas","z59","g12","f70","yzn","n81","r2w1","vu","hd"};
		DBApp.createTable("v4948", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("v4948"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test7TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"lq8","on2ln"};
		DBApp.createTable("j9q3c", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("j9q3c"));
		String[] cols1 = {"a52a"};
		DBApp.createTable("l579", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("l579"));
		String[] cols2 = {"j8o39","tg0","aos73","w9","mm3","u6u"};
		DBApp.createTable("t55gt", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("t55gt"));
		String[] cols3 = {"tqgv","d8fb","vsf","t402","x0","go0","y1","rpj38","qc84"};
		DBApp.createTable("d20k", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("d20k"));
		String[] cols4 = {"aob0","w9","sc3do","uzw","t7","b06","k3306"};
		DBApp.createTable("czt", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("czt"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test8TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"j9d"};
		DBApp.createTable("v3l", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("v3l"));
		String[] cols1 = {"etp36","md3","p8vi","jm6","i2b","j5yv","rmg8","m2","qa","lng"};
		DBApp.createTable("uv8n", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("uv8n"));
		String[] cols2 = {"p2","v0","gc5","b8j","fp2","lac"};
		DBApp.createTable("b8", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("b8"));
		String[] cols3 = {"d9cy","l3"};
		DBApp.createTable("f0", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("f0"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test9TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"qor","t4u","wx","i7z","c8","ymyp"};
		DBApp.createTable("mc", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("mc"));
		String[] cols1 = {"u96"};
		DBApp.createTable("i2", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("i2"));
		String[] cols2 = {"ebo","ih7s","axm","iep67"};
		DBApp.createTable("v8h1g", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("v8h1g"));
		String[] cols3 = {"ver","a69","gq730","i7","obxz"};
		DBApp.createTable("a9", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("a9"));
		String[] cols4 = {"x5a","xn"};
		DBApp.createTable("q64", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("q64"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test10TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"e0v","c0d80","b95x0","hc0","zf","k7","q74q","l0vmh"};
		DBApp.createTable("ni0s", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ni0s"));
		String[] cols1 = {"g00","l4u","ph"};
		DBApp.createTable("rqa", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("rqa"));
		String[] cols2 = {"e99mg"};
		DBApp.createTable("gz1", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("gz1"));
		String[] cols3 = {"us","l5m","g60","d0x","m0ms0","tx448","vyd5"};
		DBApp.createTable("bg3m", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("bg3m"));
		String[] cols4 = {"kn","hkp","ic","v064j","zsso","v22m"};
		DBApp.createTable("r9n", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("r9n"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test11TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"t02","cu89","y5fm","dbzw","hpj","mdgqv"};
		DBApp.createTable("mi6", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("mi6"));
		String[] cols1 = {"j3b1","l2","uh","fk76"};
		DBApp.createTable("x30a", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("x30a"));
		String[] cols2 = {"r8k","x329","u1","kjz","l1273","ntj23","y1c","w3"};
		DBApp.createTable("w42", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("w42"));
		String[] cols3 = {"ql","gd636","yij"};
		DBApp.createTable("h35", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("h35"));
		String[] cols4 = {"m9","zwvg","r80","d0q","y7b","cc2","mg","b8u6"};
		DBApp.createTable("a7", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("a7"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test12TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"orjy","k8b5i"};
		DBApp.createTable("fz5", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("fz5"));
		String[] cols1 = {"i6t","xng","q9kk","a8","brp2x","mo","fc0","qt37y","c79dm","k7i"};
		DBApp.createTable("y2p", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("y2p"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test13TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"c7","a9fg","zq3w","l8k","w02","oqqw"};
		DBApp.createTable("m665u", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("m665u"));
		String[] cols1 = {"t00c","ug0","w0","hsb"};
		DBApp.createTable("bz", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("bz"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test14TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"m87","h5d","w1"};
		DBApp.createTable("fs6e3", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("fs6e3"));
		String[] cols1 = {"d1","bz6","b3l0","uk3","g88","z66","e144"};
		DBApp.createTable("dsxw3", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("dsxw3"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test15TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"o20","t6","ptuo","c0","utxxb","au48"};
		DBApp.createTable("vn5m", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("vn5m"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test16TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"p129","v24","k9","lhhj7","kjk","nx","dkn"};
		DBApp.createTable("p47p", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("p47p"));
		String[] cols1 = {"ugx","y95","od3","c2o","gx9v","r6ut"};
		DBApp.createTable("c07q", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("c07q"));
		String[] cols2 = {"ye7","gvr6","g2m","sj0n5","iig8v","rz8l8","b2","ju7"};
		DBApp.createTable("ojk", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("ojk"));
		String[] cols3 = {"qb7vs","y7"};
		DBApp.createTable("p64c8", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("p64c8"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test17TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"dl"};
		DBApp.createTable("j6q7", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("j6q7"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test18TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"tl7k","p64","nvv","e5w3","b8u","o22z"};
		DBApp.createTable("x5l6", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("x5l6"));
		String[] cols1 = {"k0"};
		DBApp.createTable("q30", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("q30"));
		String[] cols2 = {"db","d7o7e","aq4zh","ccf","i6m"};
		DBApp.createTable("k7l", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("k7l"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test19TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"z00o","t5","q0w","wbs","c3mlg","qf8","e10"};
		DBApp.createTable("q67", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("q67"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test20TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"drc4","r8","k1585","fm9h"};
		DBApp.createTable("i5", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("i5"));
		String[] cols1 = {"ho","rs77"};
		DBApp.createTable("hs", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("hs"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test21TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"fb","l7n","u47gd","u4ym","p9a4","m0"};
		DBApp.createTable("tk", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("tk"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test22TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"eii","tz2jv","kts","ue6p5","i7lk","t9","tr5k","qj","vm7","y6"};
		DBApp.createTable("ir8", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ir8"));
		String[] cols1 = {"iv3","p2nv","z0h","m7kj6","h7nk","rvmt5","q0"};
		DBApp.createTable("ej9b", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("ej9b"));
		String[] cols2 = {"fcrb9","ee","p565m","f6","xy3j1","bdw","z755k","y94","ct","uv4qz"};
		DBApp.createTable("br", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("br"));
		String[] cols3 = {"q6","a88"};
		DBApp.createTable("t075n", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("t075n"));
		String[] cols4 = {"m3j"};
		DBApp.createTable("g83", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("g83"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test23TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"c54","q45b6","lh5"};
		DBApp.createTable("h023i", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("h023i"));
		String[] cols1 = {"a54fd","hw","dg","cw61s"};
		DBApp.createTable("r7v6", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("r7v6"));
		String[] cols2 = {"s6","rqx","mb","c8j"};
		DBApp.createTable("hz0", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("hz0"));
		String[] cols3 = {"m2t","hzr","c624","v91","f800","n69","e99"};
		DBApp.createTable("s7", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("s7"));
		String[] cols4 = {"z5c","c7yr","ensnt","ou","hxr","o54","u0","buc5"};
		DBApp.createTable("n16qk", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("n16qk"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test24TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"rl","y62","r03","v7dp6","i7u","ak1"};
		DBApp.createTable("ba1", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ba1"));
		String[] cols1 = {"u9g","t3","z1td3"};
		DBApp.createTable("cds00", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("cds00"));
		String[] cols2 = {"nre73","wmy","uyb","qy","dmv2","de"};
		DBApp.createTable("i4159", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("i4159"));
		String[] cols3 = {"kvhi0","g55","nz6","o3l9","uw","hnbu6","x5k6y","nwh1"};
		DBApp.createTable("swx", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("swx"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test25TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"sqe9","g08","p6","h2j","xv","o3j","jbu"};
		DBApp.createTable("bf", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("bf"));
		String[] cols1 = {"u5","wzq","ceg","m8uke","uk","ltbv","r6aj2","k3","nfr0","s5b4a"};
		DBApp.createTable("l55", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("l55"));
		String[] cols2 = {"n5","g68","s0g"};
		DBApp.createTable("pw64z", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("pw64z"));
		String[] cols3 = {"rta","f7ud"};
		DBApp.createTable("y63", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("y63"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test26TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"h31c0","qc"};
		DBApp.createTable("kmh", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("kmh"));
		String[] cols1 = {"cm31","ed0","i3v","dcb"};
		DBApp.createTable("aj07", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("aj07"));
		String[] cols2 = {"vrbs"};
		DBApp.createTable("c59v", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("c59v"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test27TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"lk9y","p60","g6k2","ll1","k99","h99","s5v6","c3","j29","r5s"};
		DBApp.createTable("a7feg", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("a7feg"));
		String[] cols1 = {"wc","fti","z414","pd","n710","cdb"};
		DBApp.createTable("xkx", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("xkx"));
		String[] cols2 = {"p2","yy","m2","msd","s9l8","cshc","h1iff"};
		DBApp.createTable("w57", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("w57"));
		String[] cols3 = {"a8","xgn71","ldg2","z60m","or8","c7mr5"};
		DBApp.createTable("bc6bj", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("bc6bj"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test28TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"n5470","b81","vj1","p74m4"};
		DBApp.createTable("fcb74", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("fcb74"));
		String[] cols1 = {"e8571"};
		DBApp.createTable("mbo3b", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("mbo3b"));
		String[] cols2 = {"cd7","g9","j8","b85y","x8","r7l","p63","dyuol","wz6q"};
		DBApp.createTable("ip8g", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("ip8g"));
		String[] cols3 = {"zo","vd1","e923","fk","s3azz","x1b"};
		DBApp.createTable("t9gj", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("t9gj"));
		String[] cols4 = {"d7xp","it","g313","uen3","g4c","tut","w10","lqv5c"};
		DBApp.createTable("b16", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("b16"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test29TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"giw","tl6t","uw","m4835","p8"};
		DBApp.createTable("ha5qq", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ha5qq"));
		String[] cols1 = {"be2","a8","kman","w45zt","k4c","fh","qx5","m9q","e7p"};
		DBApp.createTable("ctu4", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("ctu4"));
		String[] cols2 = {"l0","jl0v5","s0qg","o1y7"};
		DBApp.createTable("kz9", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("kz9"));
		String[] cols3 = {"i81zh"};
		DBApp.createTable("x61i", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("x61i"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test30TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"m70","qz","jp46p","l4un","qm22","wzr","ph1f6","z70ym"};
		DBApp.createTable("o7tn", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("o7tn"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test31TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"yc5r","jz","j2s"};
		DBApp.createTable("bznk", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("bznk"));
		String[] cols1 = {"ib4","fsq56","twd","ev3e8","ed3","y6vk","d8n"};
		DBApp.createTable("g6", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("g6"));
		String[] cols2 = {"zdb","tj6dw","lr51","d1l","gb","w3vc","a71"};
		DBApp.createTable("t8660", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("t8660"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test32TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"ex","gvb9w","pt19","ye9","f4","hnm5","w81","ffid"};
		DBApp.createTable("m987", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("m987"));
		String[] cols1 = {"e5lj","u1o","j4ig","bo","x99b","f63"};
		DBApp.createTable("cv83f", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("cv83f"));
		String[] cols2 = {"co"};
		DBApp.createTable("l3f", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("l3f"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test33TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"bl2sv","er5r8","z8e","g8p","c3e22"};
		DBApp.createTable("l88", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("l88"));
		String[] cols1 = {"qd","ul27","f37","g0","zihb1"};
		DBApp.createTable("s00", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("s00"));
		String[] cols2 = {"tetg","tvlm3","x8"};
		DBApp.createTable("fa", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("fa"));
		String[] cols3 = {"g392","dn","rvx6","z09v"};
		DBApp.createTable("jb7", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("jb7"));
		String[] cols4 = {"nc4","hc8","i7cy","b2x5u","k482r","yx2"};
		DBApp.createTable("ct8", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("ct8"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test34TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"qw6","d2s","bfg","fr1","w777","wto","fx","pb49","m93vx"};
		DBApp.createTable("zhwme", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("zhwme"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test35TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"wyq1","zhga","cz1","neec2"};
		DBApp.createTable("oh7", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("oh7"));
		String[] cols1 = {"f06","az","t80","yet","x2g","g7wqv","say","gdv"};
		DBApp.createTable("qb", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("qb"));
		String[] cols2 = {"hk058","mg4","zk2j","a8t53","aq9z","npq6a"};
		DBApp.createTable("ae54", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("ae54"));
		String[] cols3 = {"ozs","yz","r729","rmsmp","k5x"};
		DBApp.createTable("h3j", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("h3j"));
		String[] cols4 = {"bq","w27","j2h0","y6y34","sn4","cu","hr","lm9my"};
		DBApp.createTable("kab", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("kab"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test36TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"jqk87","c5ph9","gv0"};
		DBApp.createTable("b0", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("b0"));
		String[] cols1 = {"ai4","tx2e"};
		DBApp.createTable("g5m", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("g5m"));
		String[] cols2 = {"gk4","k7q","ftn","orr5","b1","ep4","d4qo"};
		DBApp.createTable("ot5", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("ot5"));
		String[] cols3 = {"gg67","aj7","t60","r73","trl","xdm","yed","e706a"};
		DBApp.createTable("c83", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("c83"));
		String[] cols4 = {"d3t31"};
		DBApp.createTable("p2k", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("p2k"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test37TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"z6y","q302f","x6c","sb6it","dt","q80s2"};
		DBApp.createTable("wm8", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("wm8"));
		String[] cols1 = {"y6","ag","xl0","i43l","bs5","y9o","r3tz8","vi10","c97p6"};
		DBApp.createTable("t03", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("t03"));
		String[] cols2 = {"zp7","ai","o61","c55"};
		DBApp.createTable("e1n", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("e1n"));
		String[] cols3 = {"cu2","gu","j0h","sj","e39l","i81","qy","q997","y6e","s1z"};
		DBApp.createTable("vb9", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("vb9"));
		String[] cols4 = {"gr4","hd8i","n1","pd","z5","ggr","lc8y","c0","m1"};
		DBApp.createTable("b9t", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("b9t"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test38TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"ge5i","nf","u132","b913"};
		DBApp.createTable("xq", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("xq"));
		String[] cols1 = {"m6d","a1c","vf","oa","ljpl","qx5","u6r9l","q37"};
		DBApp.createTable("zijr", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("zijr"));
		String[] cols2 = {"h704","k95","u45","rini"};
		DBApp.createTable("t20a7", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("t20a7"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test39TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"m9i","ikg","o8l4","bw","t0y1","uc4","f7g"};
		DBApp.createTable("kl6lw", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("kl6lw"));
		String[] cols1 = {"etru","h8qz3","ii4b","wvc","s2h5","mos","u3kl","m492h"};
		DBApp.createTable("b89o9", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("b89o9"));
		String[] cols2 = {"b47xw","z60r","sv0","lb","d41","jt","zjo2"};
		DBApp.createTable("oql", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("oql"));
		String[] cols3 = {"w6z","o3x","y1","c76","r9","jb7","r1h1x"};
		DBApp.createTable("y24", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("y24"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test40TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"n9g8","or","sxf","ix","kx","fc3o4","hb8","y95e1","bi"};
		DBApp.createTable("yb", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("yb"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test41TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"l59","b8b","t4","os","iwe","pjn0i","lz","txp","qc954"};
		DBApp.createTable("sp5", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("sp5"));
		String[] cols1 = {"pgm","v21","ze320","jq","en"};
		DBApp.createTable("e24i5", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("e24i5"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test42TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"u3","i5b","n92r","gg","h3ig"};
		DBApp.createTable("kza", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("kza"));
		String[] cols1 = {"r1z6","id3","ylg","nx6b","iry","mq15","s087","hxg5b","f9","r3v"};
		DBApp.createTable("hf3r5", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("hf3r5"));
		String[] cols2 = {"xyyel","zs2o6","w5c","u557"};
		DBApp.createTable("cd949", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("cd949"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test43TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"jok6","zzm","trl34","q7l","pb3y","j5lh9","j01"};
		DBApp.createTable("xk4", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("xk4"));
		String[] cols1 = {"lt8","e46","p7f8","f9m","rug0","v2z","juy2m","kg2","c7cb0"};
		DBApp.createTable("s19gj", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("s19gj"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test44TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"izw","c88","bm0"};
		DBApp.createTable("psbl", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("psbl"));
		String[] cols1 = {"njc","zlif8","o3u5s"};
		DBApp.createTable("xk", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("xk"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test45TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"fs8","ltb","pw","t1wrk","t9f"};
		DBApp.createTable("ldl", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ldl"));
		String[] cols1 = {"b56","e92x2","q5","yt90"};
		DBApp.createTable("w555", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("w555"));
		String[] cols2 = {"bx","f74ku","ivm7","u6l","r3p","x1h"};
		DBApp.createTable("f52", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("f52"));
		String[] cols3 = {"b3kib","l0y","uq8x","b3","w62","wz6","w679","m98","w7c","r8"};
		DBApp.createTable("v36e", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("v36e"));
		String[] cols4 = {"vx9","v7","i96r","i4908","u23","oq28","u1h","e9gz","v527"};
		DBApp.createTable("fu1", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("fu1"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test46TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"er1","m76","gxc","s6","fzs","da","j3","oq","dl"};
		DBApp.createTable("lr", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("lr"));
		String[] cols1 = {"ub","yu8","e86","dx","d4","as7","siru","lqs","g2b"};
		DBApp.createTable("pr2", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("pr2"));
		String[] cols2 = {"vt6mz","zo7b8","ights","a3at","vqo","mf9a","mle","jus4","r4","f0h"};
		DBApp.createTable("f0", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("f0"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test47TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"owo3"};
		DBApp.createTable("tw0o3", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("tw0o3"));
		String[] cols1 = {"m5","nhymo"};
		DBApp.createTable("o0d", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("o0d"));
		String[] cols2 = {"tw","fcn2","rq98l","iay"};
		DBApp.createTable("axs", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("axs"));
		String[] cols3 = {"ue896","tev","u1ax","e8m9","ax30y","dx5f"};
		DBApp.createTable("gm49", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("gm49"));
		String[] cols4 = {"q4u","og"};
		DBApp.createTable("dz4", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("dz4"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test48TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"awf","f2v4","hr","b3w","w80","z4v"};
		DBApp.createTable("n9g60", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("n9g60"));
		String[] cols1 = {"s65"};
		DBApp.createTable("g6yno", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("g6yno"));
		String[] cols2 = {"a8","l5v","f83","n3vlm","p06d4","d4","mp8"};
		DBApp.createTable("f5", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("f5"));
		String[] cols3 = {"wj9rq"};
		DBApp.createTable("o2z", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("o2z"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test49TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"yg","m97p"};
		DBApp.createTable("o66d1", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("o66d1"));
		String[] cols1 = {"y7x","z4","t454","pc0","t9ob","zk2e"};
		DBApp.createTable("got79", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("got79"));
		String[] cols2 = {"by4s","lb","cy8","z0n9"};
		DBApp.createTable("v11", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("v11"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test50TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"k57t1","n7sk","v8j8","d40","u268","g8","zgf67","n8e"};
		DBApp.createTable("e7", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("e7"));
		String[] cols1 = {"t4v"};
		DBApp.createTable("lsu", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("lsu"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test51TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"xr3","b4","i5q","m6"};
		DBApp.createTable("pd5", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("pd5"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test52TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"v3j8","gy","kcm","y2kq9","t0q","w4e9","dc","gu","h52"};
		DBApp.createTable("of", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("of"));
		String[] cols1 = {"y2y5m","vmwfo"};
		DBApp.createTable("xhb", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("xhb"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test53TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"h04","r1j","dxwv","pq"};
		DBApp.createTable("ju3", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ju3"));
		String[] cols1 = {"fix1","vps","ow7","ru34","l3pcf","d8y","rv9","lr5","e1"};
		DBApp.createTable("c01", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("c01"));
		String[] cols2 = {"dahz","bhst","vq3o","fn48","ydd0","v8"};
		DBApp.createTable("pm9as", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("pm9as"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test54TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"m07","cr","e2x","h571","w1t","i0uy","th"};
		DBApp.createTable("v76", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("v76"));
		String[] cols1 = {"qso","h794","p5u"};
		DBApp.createTable("g91", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("g91"));
		String[] cols2 = {"e86v5","u0","o9o","r3","uh","rf"};
		DBApp.createTable("xm", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("xm"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test55TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"r9","q92st","p5975","t6g","l758","kxr8w","hwh"};
		DBApp.createTable("y5", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("y5"));
		String[] cols1 = {"gn7","ude","g5f","gt4","lxmz","u910"};
		DBApp.createTable("a6", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("a6"));
		String[] cols2 = {"i25","le","s5b"};
		DBApp.createTable("m69", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("m69"));
		String[] cols3 = {"nk","wl4","o436","t15n","ms96v"};
		DBApp.createTable("qq2", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("qq2"));
		String[] cols4 = {"qf","d8i","w6n","e6yjx","j4gzk","v4z","ixg","cm","mya","f04tn"};
		DBApp.createTable("be", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("be"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test56TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"oj5","g29","lr43i","oos","a1p51","i1","ayv4","kuy","k2b4"};
		DBApp.createTable("a5p", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("a5p"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test57TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"g7","a4","ef","r0m","q1","u0sh"};
		DBApp.createTable("b4", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("b4"));
		String[] cols1 = {"qj","b1894","r2","to63","rz2","fxy","r01z"};
		DBApp.createTable("np", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("np"));
		String[] cols2 = {"z2y","y9","c2","a8h","qo988","d88a5","h96h","oy","f41y","s7"};
		DBApp.createTable("zl", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("zl"));
		String[] cols3 = {"av32g","frhoq","me01","e3r5s","iqr6","pc"};
		DBApp.createTable("o8", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("o8"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test58TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"he","clm3f","e9z","q2","qw1","x1","r1","aflvc","e57"};
		DBApp.createTable("k03c", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("k03c"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test59TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"fjn0","i9","bbnr8","b366","wl5","t27v","z86","yba","d0"};
		DBApp.createTable("f26b", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("f26b"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test60TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"n27","ws3","laet3","l50w","hc","z8txt","lb","vafs5","g0h","i75"};
		DBApp.createTable("at0", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("at0"));
		String[] cols1 = {"d8hh","pq5","xj6","oa","d7","m5a","z79v1","o7"};
		DBApp.createTable("rv081", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("rv081"));
		String[] cols2 = {"w95","n415n","q10","e4","ae70","a7h","iy","px3","n7m","n6w8"};
		DBApp.createTable("oq5", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("oq5"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test61TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"j0mq7","u9j","j15","ye8r3","uu","tr78"};
		DBApp.createTable("uybb", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("uybb"));
		String[] cols1 = {"kj","d0o9y","e6b6","l13"};
		DBApp.createTable("cs48v", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("cs48v"));
		String[] cols2 = {"v55"};
		DBApp.createTable("y4qt0", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("y4qt0"));
		String[] cols3 = {"x9n","gq3"};
		DBApp.createTable("kc9", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("kc9"));
		String[] cols4 = {"l53x","vt3","wf6k0"};
		DBApp.createTable("a4pm9", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("a4pm9"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test62TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"lc","hsv","fl","b15","po","o6n","l08gr","hf2","o42"};
		DBApp.createTable("d8x", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("d8x"));
		String[] cols1 = {"fko","n6","l9362","eb4","ak3x","kwa"};
		DBApp.createTable("v4i", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("v4i"));
		String[] cols2 = {"esp","w95","bxb","b75w","x5c"};
		DBApp.createTable("cn8", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("cn8"));
		String[] cols3 = {"a5y2","kms","k0p33","awh","eq3","td67","n91","k7v","p99"};
		DBApp.createTable("mwb2", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("mwb2"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test63TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"lj"};
		DBApp.createTable("rfhj", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("rfhj"));
		String[] cols1 = {"jjpuz","jwcu"};
		DBApp.createTable("k7", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("k7"));
		String[] cols2 = {"o6","cq","ae4"};
		DBApp.createTable("finj", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("finj"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test64TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"k4y"};
		DBApp.createTable("lvp", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("lvp"));
		String[] cols1 = {"eock7","a3cz","h5jq","yaz","l3rq7","qb4l0","u6h","q13","xd80k"};
		DBApp.createTable("g1e", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("g1e"));
		String[] cols2 = {"m34","u1lv","m5t0","p8","bo1","ruw","w8u7f","hh"};
		DBApp.createTable("oa2", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("oa2"));
		String[] cols3 = {"t5","xc","m22","qk0","ko5l","u1z","egj","als","j5l8","yz4h9"};
		DBApp.createTable("p6j", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("p6j"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test65TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"gu1","v2","ttw3","zs","c4q","e1ln"};
		DBApp.createTable("xfu9", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("xfu9"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test66TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"ps","k77","zx0","v9m","b52","n43r7"};
		DBApp.createTable("mnwx6", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("mnwx6"));
		String[] cols1 = {"kek1a","b806k","l6uei","z4u","bi373","j1952","j6w","k00","m481"};
		DBApp.createTable("se259", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("se259"));
		String[] cols2 = {"rix9","dgf","ko67f","w9d","vf2","oa58","y0o","rd1","cb0","d419"};
		DBApp.createTable("va58l", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("va58l"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test67TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"us","fl8e5","u1e","b8a","u98"};
		DBApp.createTable("t9pq", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("t9pq"));
		String[] cols1 = {"d4l4c","ko","b2","fgn","lo"};
		DBApp.createTable("dl", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("dl"));
		String[] cols2 = {"hn0t1","u5","b8"};
		DBApp.createTable("sa8", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("sa8"));
		String[] cols3 = {"w1gq","x06v0","f45o"};
		DBApp.createTable("s483", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("s483"));
		String[] cols4 = {"yw3k0","nf3a9","g7","b1","a16","a9zv2","w73v2","vwi1m","e20"};
		DBApp.createTable("bs9d", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("bs9d"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test68TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"xyb2","m9e","z18f","sgv"};
		DBApp.createTable("yoyd4", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("yoyd4"));
		String[] cols1 = {"gf","g3s","oe9"};
		DBApp.createTable("d5u4", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("d5u4"));
		String[] cols2 = {"c83","qtc","r9","ic6ep","oaco","f187n","g9ls1","hgw6"};
		DBApp.createTable("ihq", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("ihq"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test69TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"qn945","bik","sz2","jypo"};
		DBApp.createTable("w87", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("w87"));
		String[] cols1 = {"h4vc"};
		DBApp.createTable("cug", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("cug"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test70TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"r168","jsx","hjp41","mp18","z0j"};
		DBApp.createTable("dyc", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("dyc"));
		String[] cols1 = {"y97","n06f9","c4zr3","r8l","sba","s8"};
		DBApp.createTable("cl93", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("cl93"));
		String[] cols2 = {"et6a","zp02w","zja","w6401","s0h","n0","be3a8","mvu","q45n","mn"};
		DBApp.createTable("t4o", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("t4o"));
		String[] cols3 = {"ur52","v2g7","jw6","wc","l2d"};
		DBApp.createTable("fg8v4", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("fg8v4"));
		String[] cols4 = {"q5b0","zh509","pnl","lke","v7953"};
		DBApp.createTable("u8ij", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("u8ij"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test71TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"u1t","slll8","w06","z3sy","hp","ba","d99k","zcr","mo76"};
		DBApp.createTable("ab3q", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ab3q"));
		String[] cols1 = {"bp81","sre","w526b","n0e6","g59","gx","a09s","u8","lgbd3"};
		DBApp.createTable("uc12", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("uc12"));
		String[] cols2 = {"e819","u1f","a1j","oy4","q2r00"};
		DBApp.createTable("xeh4", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("xeh4"));
		String[] cols3 = {"mq7","vqs5","dww"};
		DBApp.createTable("d7k6", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("d7k6"));
		String[] cols4 = {"d3l","bbtjf","v8","gfs","x1k9","p0l8r"};
		DBApp.createTable("qsxv3", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("qsxv3"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test72TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"oj56","t1j","ua2","fz9ht","a8r","o7","l75","o048","jt"};
		DBApp.createTable("dx55", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("dx55"));
		String[] cols1 = {"kd","uep","g98"};
		DBApp.createTable("c76", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("c76"));
		String[] cols2 = {"el0","j5","i67","tvq"};
		DBApp.createTable("ve", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("ve"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test73TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"l07oa"};
		DBApp.createTable("ql", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ql"));
		String[] cols1 = {"cs4","ykv","girv","te","ec732","ydy"};
		DBApp.createTable("nx1", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("nx1"));
		String[] cols2 = {"d79","rh7","r8"};
		DBApp.createTable("h8kk2", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("h8kk2"));
		String[] cols3 = {"byn","ygjg","k2f7"};
		DBApp.createTable("b7s", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("b7s"));
		String[] cols4 = {"zj649","b6"};
		DBApp.createTable("idqb", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("idqb"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test74TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"nux","pem","ylv","d77","wbhh","to"};
		DBApp.createTable("b0", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("b0"));
		String[] cols1 = {"p1f","b3xp","ehjk3","t87","mh8n"};
		DBApp.createTable("z700", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("z700"));
		String[] cols2 = {"sc5","c30","w5py3","w1s","a5a"};
		DBApp.createTable("poq8", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("poq8"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test75TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"p6l","mzi","l2m"};
		DBApp.createTable("g7i1", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("g7i1"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test76TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"srz","h9r2","w4a"};
		DBApp.createTable("p75", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("p75"));
		String[] cols1 = {"x0e","l4","p2","i2z5","h8427","pxt","teac","c1w","e4","aboi"};
		DBApp.createTable("lie8", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("lie8"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test77TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"qg7b","j1ms5","a47"};
		DBApp.createTable("ho3w2", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ho3w2"));
		String[] cols1 = {"szsg7","w03"};
		DBApp.createTable("z6n", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("z6n"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test78TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"pi","k45","at514","jx3"};
		DBApp.createTable("ui", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ui"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test79TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"t5","d6","bg","g365"};
		DBApp.createTable("p61", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("p61"));
		String[] cols1 = {"m5","o2m2","fb1fn"};
		DBApp.createTable("qpp", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("qpp"));
		String[] cols2 = {"kz9zx","a5k","y4ko","ln5","n2z"};
		DBApp.createTable("uk", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("uk"));
		String[] cols3 = {"j1646","nxs","bhz","e13","v3","rz6l","o7z","mn0","o9wl"};
		DBApp.createTable("u989", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("u989"));
		String[] cols4 = {"b77"};
		DBApp.createTable("hk", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("hk"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test80TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"rma6x","k57e3","oly"};
		DBApp.createTable("lz", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("lz"));
		String[] cols1 = {"x51"};
		DBApp.createTable("i16", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("i16"));
		String[] cols2 = {"p4k","smi","fkqn2","f06b","hg84"};
		DBApp.createTable("rb3", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("rb3"));
		String[] cols3 = {"yc","x3","m79l"};
		DBApp.createTable("w3", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("w3"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test81TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"zc3zr","i5","y661","gvdi","qb","b3t48"};
		DBApp.createTable("kls0", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("kls0"));
		String[] cols1 = {"gg0"};
		DBApp.createTable("efh1k", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("efh1k"));
		String[] cols2 = {"ir","ko","s7rwn","s49","ro","m709","ph5","vu449"};
		DBApp.createTable("i7f", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("i7f"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test82TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"h46","ul4g3","kn1","g80","z4z","vb23"};
		DBApp.createTable("dc", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("dc"));
		String[] cols1 = {"d550n","ih5","ed0u","rs67","m52","vl"};
		DBApp.createTable("y9e", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("y9e"));
		String[] cols2 = {"g1y75","bf3","x4277","av7","a30","l9","y1","f5p3","uwv","id"};
		DBApp.createTable("vr87n", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("vr87n"));
		String[] cols3 = {"pn4","x2o01","pgk","x4e","z63c"};
		DBApp.createTable("vei6", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("vei6"));
		String[] cols4 = {"yd41g","w2s3r"};
		DBApp.createTable("a0", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("a0"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test83TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"u0","hi","dfab","nce","j9k","he0","ynjlw"};
		DBApp.createTable("r85m", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("r85m"));
		String[] cols1 = {"b39","xs","ixze","fw5bt","z7p","cp2rq","pzz","d6e","z7e","jp8"};
		DBApp.createTable("r279j", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("r279j"));
		String[] cols2 = {"g402","kbt9c","l69","ph94","iszp5","e1z","f7o","g7zz","z7m","d69q"};
		DBApp.createTable("el976", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("el976"));
		String[] cols3 = {"e0ls3","v92z","vm20"};
		DBApp.createTable("rym02", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("rym02"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test84TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"ys","o1d","kax","m0dzn","m1","j5h31","u29"};
		DBApp.createTable("ffl", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ffl"));
		String[] cols1 = {"zlu1","n7n","s49","wxo85","dv","hm","p4i3","t6471"};
		DBApp.createTable("sr", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("sr"));
		String[] cols2 = {"d7","g23","g7mjn","g14","k36a","xh9y"};
		DBApp.createTable("hh5", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("hh5"));
		String[] cols3 = {"ug","v0gz2","hfc","u79","g79","fy2"};
		DBApp.createTable("f4n", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("f4n"));
		String[] cols4 = {"dr","h70xx","ui0l0","b49"};
		DBApp.createTable("js2", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("js2"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test85TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"l3"};
		DBApp.createTable("r2q", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("r2q"));
		String[] cols1 = {"o0h","f5","k22c"};
		DBApp.createTable("did97", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("did97"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test86TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"bv41","ssr","ad34","la5","v99","lx7h"};
		DBApp.createTable("hyr", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("hyr"));
		String[] cols1 = {"l3s","ybt","i843","a7","o3","pef"};
		DBApp.createTable("e7ii", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("e7ii"));
		String[] cols2 = {"i1l","th","nw","k2eq","apd","ed093","v69","o7367","g2","ri7y"};
		DBApp.createTable("ft7s3", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("ft7s3"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test87TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"qqa35","g4h","akx","e5"};
		DBApp.createTable("c5", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("c5"));
		String[] cols1 = {"r2gj","ufryd","ar3","qd3"};
		DBApp.createTable("g3", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("g3"));
		String[] cols2 = {"dy5a","f4","bi","c4","pxil","h6i","y1jz"};
		DBApp.createTable("d29", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("d29"));
		String[] cols3 = {"n70j8","cp7","v31","p03","k1","vp","qdl","xi4","f63aq"};
		DBApp.createTable("neh", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("neh"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test88TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"fz","z950","y24al","m10","r61ig"};
		DBApp.createTable("w8", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("w8"));
		String[] cols1 = {"pn1","f0","tgm"};
		DBApp.createTable("ez0", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("ez0"));
		String[] cols2 = {"tn","a2k","vo7","r91g"};
		DBApp.createTable("k0", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("k0"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test89TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"u57","d4xy","ppl","qh9","r34","v13x"};
		DBApp.createTable("mkk", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("mkk"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test90TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"p76","eb6","ru","nt16","z45","p1ic","ma","e2hqf"};
		DBApp.createTable("ab8", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ab8"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test91TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"np174","z3x","auo","km8","eva5","tn3"};
		DBApp.createTable("vwq1s", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("vwq1s"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test92TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"hx","pwo","kz1gl","e679","ml","yb4","xjh","t6w","z7w","m923v"};
		DBApp.createTable("nj8", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("nj8"));
		String[] cols1 = {"a8","x1s9","g4","ew2","bzz","b60","cox2l","ofu","od0","yxm6p"};
		DBApp.createTable("l15f", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("l15f"));
		String[] cols2 = {"a9n33","o950","b5v8k"};
		DBApp.createTable("g93", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("g93"));
		String[] cols3 = {"zwn9","kx","nbb","p3j38","r09","w5","p73","j7"};
		DBApp.createTable("il9", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("il9"));
		String[] cols4 = {"t6m","y8d","yr","s98d1","sysq","mk7","r14m1","ct9","czm","ww"};
		DBApp.createTable("c6k65", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("c6k65"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test93TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"f08"};
		DBApp.createTable("ou9m1", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ou9m1"));
		String[] cols1 = {"v6a","c30","nvp3","sst"};
		DBApp.createTable("s15", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("s15"));
		String[] cols2 = {"xx7","c94","h1mni","e9","i5rsi","dgda8","c5q","n449","k4b","hz18"};
		DBApp.createTable("dcs", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("dcs"));
		String[] cols3 = {"bmt","u58"};
		DBApp.createTable("a6ax", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("a6ax"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test94TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"z75","f0z","v5","cr4b","qw1","q6","xv"};
		DBApp.createTable("l27c", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("l27c"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test95TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"nza8","kpn","dz","p6gu","w1cgt","x4tol","qhdu"};
		DBApp.createTable("qnu", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("qnu"));
		String[] cols1 = {"p68","v49","sd","l1i","e8vs","n724","c7","ad4v","ayf","s9"};
		DBApp.createTable("tj5", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("tj5"));
		String[] cols2 = {"ncr","s01","uku","v1d","ert"};
		DBApp.createTable("h68", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("h68"));
		String[] cols3 = {"e9b7"};
		DBApp.createTable("yy", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("yy"));
		String[] cols4 = {"m3kb","h2","r8","u7p8","ck8","a1","j8","ew","i2z","rt0"};
		DBApp.createTable("xjb", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("xjb"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test96TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"bic0","e3k","y1o2","o6qs6","s8us","t49","j8a4","zd"};
		DBApp.createTable("cjn99", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("cjn99"));
		String[] cols1 = {"ha993","drp","lqd2h","e83","d8i","wh5","z0","o959j","cg","c98"};
		DBApp.createTable("f7", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("f7"));
		String[] cols2 = {"o08","t15","rs00","f5","hz","ho7","vz87","h7n"};
		DBApp.createTable("dh", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("dh"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test97TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"riu","kw","xjg","f6l","aq","g9fj","j7ey","ky3m","k1t4m","e8i4"};
		DBApp.createTable("hdq", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("hdq"));
		String[] cols1 = {"km","d5","pei","l80x","eb4"};
		DBApp.createTable("ol", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("ol"));
		String[] cols2 = {"x20"};
		DBApp.createTable("n9yv", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("n9yv"));
		String[] cols3 = {"co29c","n7y","f9h","r6cf","m1kl","u0","q3","vax","vi","hnka"};
		DBApp.createTable("dfj3", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("dfj3"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test98TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"g0","r43"};
		DBApp.createTable("qmx", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("qmx"));
		String[] cols1 = {"vsa","gg0","z9","p60","xi","ewz","atj1","cl6","rh"};
		DBApp.createTable("fi3", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("fi3"));
		String[] cols2 = {"hz4c","ya49","l89","fjo","w2","z818x","c2","ah07","vg401","dw33y"};
		DBApp.createTable("mt4q", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("mt4q"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test99TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"yrf9","c7","vq4o","u7"};
		DBApp.createTable("jx", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("jx"));
		String[] cols1 = {"ptj","x1c","hx1","kh00","cv","rg5"};
		DBApp.createTable("ro5", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("ro5"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test100TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"tgm","fwod","gsj88","f05"};
		DBApp.createTable("ei3s", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ei3s"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test101TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"yeoic","nh","t4b","zr3","zt4","p6o44","dfh20"};
		DBApp.createTable("tl9", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("tl9"));
		String[] cols1 = {"ef","x51","m1","g82","esrp3","vi2","z51t","m7o","w4ay3","g76"};
		DBApp.createTable("lmz7", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("lmz7"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test102TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"f6","vekk","l5y","u84","r4i","mfg5","sc4","f4h","a70y"};
		DBApp.createTable("y65mq", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("y65mq"));
		String[] cols1 = {"wo","a3a","i30","k3s","zng","xi4","yv0","ib"};
		DBApp.createTable("de", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("de"));
		String[] cols2 = {"d52","jyfz","b0","t6s","t62","r6qry","n0kf","mqx"};
		DBApp.createTable("o8z", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("o8z"));
		String[] cols3 = {"npmq5","r1o7l"};
		DBApp.createTable("x06", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("x06"));
		String[] cols4 = {"zf4","rl9","vd","y6831","zu","pwl","djs","z832","p85o"};
		DBApp.createTable("i1", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("i1"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test103TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"v7","l42","k739z","t6d","m0","g3wv","py"};
		DBApp.createTable("re9", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("re9"));
		String[] cols1 = {"r7","zp759","h8mr","us4","d26","z3o33","zzb04"};
		DBApp.createTable("cs3w5", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("cs3w5"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test104TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"pz3","p5","i89or"};
		DBApp.createTable("l3", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("l3"));
		String[] cols1 = {"a6q","z7l1","gyh","c06qp","rld3","zj3"};
		DBApp.createTable("l983", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("l983"));
		String[] cols2 = {"ep5","r49","w2","aj89","a4","yj","jrp","ojl","ah","f1m"};
		DBApp.createTable("r9", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("r9"));
		String[] cols3 = {"por3","ugi84","t8o8","a3528"};
		DBApp.createTable("z8t8c", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("z8t8c"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test105TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"anj","kh66i","kasn3","a4p","d59u","sd8","c7a17"};
		DBApp.createTable("ow7c", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ow7c"));
		String[] cols1 = {"odk4","je6h","cb69"};
		DBApp.createTable("e10", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("e10"));
		String[] cols2 = {"l1y","f3ti6"};
		DBApp.createTable("gnw", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("gnw"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test106TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"b54w","pv11","sb7","m26","m92","p389","ir","hd309"};
		DBApp.createTable("j48", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("j48"));
		String[] cols1 = {"e9","cp","v2n9","l7x","jqgy"};
		DBApp.createTable("b45q6", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("b45q6"));
		String[] cols2 = {"m9","n1979","x1r","yz23","zgu44","t7","nai4","wru","ts8a","k5l"};
		DBApp.createTable("iog", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("iog"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test107TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"hnk","vjbuy","fs","g44","gg9","y0s","hut","n1","ai0"};
		DBApp.createTable("dre", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("dre"));
		String[] cols1 = {"k857","ex0b","g53"};
		DBApp.createTable("cq050", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("cq050"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test108TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"f3u","aze"};
		DBApp.createTable("o8", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("o8"));
		String[] cols1 = {"w03"};
		DBApp.createTable("xjpd", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("xjpd"));
		String[] cols2 = {"g4","gh","l57","u0x"};
		DBApp.createTable("w6bd", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("w6bd"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test109TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"t2","w17","w4","sg","a25dc","lli","k4k7","t3","ict0d","nv8x0"};
		DBApp.createTable("ioe", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ioe"));
		String[] cols1 = {"cwb9","ln9","j2c","vv","d0u","t8o","k4f","q1"};
		DBApp.createTable("t6n", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("t6n"));
		String[] cols2 = {"nz2","e1j4"};
		DBApp.createTable("vnx8c", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("vnx8c"));
		String[] cols3 = {"ac0v6","taebi","xf5l","e71f5","g41","u8","rci3","t8bh"};
		DBApp.createTable("giazr", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("giazr"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test110TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"p6yl"};
		DBApp.createTable("hr5", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("hr5"));
		String[] cols1 = {"wi2"};
		DBApp.createTable("y96", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("y96"));
		String[] cols2 = {"y7m","z44nn","a07q3","w06gy"};
		DBApp.createTable("ua5", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("ua5"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test111TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"sj","vpko4","q7","x9m","bd3","j2hf"};
		DBApp.createTable("fo1", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("fo1"));
		String[] cols1 = {"k7rmx","e13","x04n","m608","h5b","b846s","z0m","mnul5"};
		DBApp.createTable("b5qv", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("b5qv"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test112TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"ua","p2b"};
		DBApp.createTable("x2", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("x2"));
		String[] cols1 = {"l7m","odl"};
		DBApp.createTable("hl3j", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("hl3j"));
		String[] cols2 = {"lh","m2b","rs01","lmd","cv0","zodsr","d98","j87w","a2"};
		DBApp.createTable("s4u", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("s4u"));
		String[] cols3 = {"y84","n57","uhy","w5744","q98v","u9f","l7"};
		DBApp.createTable("gr6", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("gr6"));
		String[] cols4 = {"mc9","fnf3n","ki48","exq","j2","vr","o0jjd"};
		DBApp.createTable("i2e", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("i2e"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test113TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"p01","mr","d4nkf","a13","l9x","cukm","k1","a731","y7","pmd"};
		DBApp.createTable("rx", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("rx"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test114TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"ruj","m4","dm8","a8","xdil1","y9","y0","r3t1"};
		DBApp.createTable("m6v2", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("m6v2"));
		String[] cols1 = {"h47","ku06","rj","t45","kl","itw","o69"};
		DBApp.createTable("j63", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("j63"));
		String[] cols2 = {"lmd2m","eic","ggw","ju13","z55","d49","ybt","z3"};
		DBApp.createTable("r7", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("r7"));
		String[] cols3 = {"l28ty","in9","wu3","g8k","givp","w6c","mfk","b68rb"};
		DBApp.createTable("ltm9", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("ltm9"));
		String[] cols4 = {"u3","uc","j9f"};
		DBApp.createTable("a0", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("a0"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test115TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"b68","w6i3h"};
		DBApp.createTable("g9w6", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("g9w6"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test116TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"o6"};
		DBApp.createTable("a22", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("a22"));
		String[] cols1 = {"l4m","yu","fzm86","ufpx","f5n"};
		DBApp.createTable("u63", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("u63"));
		String[] cols2 = {"l74","j9x14","vh","zw","r7avf","p0","h0v0","x76","i2045","e1i7s"};
		DBApp.createTable("nhy9", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("nhy9"));
		String[] cols3 = {"n400","ry","rkr","v03","cl","hn","p9r","py"};
		DBApp.createTable("zxm", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("zxm"));
		String[] cols4 = {"xwbex","k9","l1f","e2j4c","qa8o","m17l","gje6h","se4k0"};
		DBApp.createTable("eeu", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("eeu"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test117TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"h1y47","f3v","o06","g039","w8766"};
		DBApp.createTable("lw3z", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("lw3z"));
		String[] cols1 = {"g0"};
		DBApp.createTable("aw3", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("aw3"));
		String[] cols2 = {"q7l","t39e5","k6i","v3","pd42q","t1","fhz5b"};
		DBApp.createTable("jw151", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("jw151"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test118TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"md2","h1k","jld8","f6","h4","agac9","f3"};
		DBApp.createTable("df6", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("df6"));
		String[] cols1 = {"g11an","f86","p23gb","gn94","adt8","mr","tcd"};
		DBApp.createTable("m4g", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("m4g"));
		String[] cols2 = {"lhw","vgb78","o88","i9y4j","epz"};
		DBApp.createTable("s5973", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("s5973"));
		String[] cols3 = {"znt1d","w520","xg","f2b"};
		DBApp.createTable("v78g", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("v78g"));
		String[] cols4 = {"mj"};
		DBApp.createTable("u3", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("u3"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test119TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"n59","r70","tvu1","fsf","j1m","hqm","z59g","l3ys"};
		DBApp.createTable("ihj", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ihj"));
		String[] cols1 = {"t5vd2","t492e","ay5z2","se4","g1l","fh7"};
		DBApp.createTable("rwg", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("rwg"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test120TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"o42hh","e1c7j","x5","jyi04","h2","b7x","wcx5"};
		DBApp.createTable("zy", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("zy"));
		String[] cols1 = {"sdw","k403j","go","jb","up3sr","fq36"};
		DBApp.createTable("e3fe6", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("e3fe6"));
		String[] cols2 = {"nqw","gs1q","ev","us","ey","u333m"};
		DBApp.createTable("nc20", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("nc20"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test121TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"us","p4h3","js","je0g","v1"};
		DBApp.createTable("uf", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("uf"));
		String[] cols1 = {"j273","f65f","rb","qcl6","p20"};
		DBApp.createTable("o60", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("o60"));
		String[] cols2 = {"y5s","ar950","qr","w1j","m4","vm9e9","c39","k739","d4"};
		DBApp.createTable("rm", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("rm"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test122TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"w025p","mt775","w89","k0v1","quv","xs8b","qp29t","i2"};
		DBApp.createTable("rh0q", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("rh0q"));
		String[] cols1 = {"buu","jio","q99","w255","q1f0","o2","l4","q9ibx","xpsj","m6"};
		DBApp.createTable("ln8", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("ln8"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test123TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"b9t79","hh","jhke","v027i"};
		DBApp.createTable("msc4m", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("msc4m"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test124TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"w1","h0f","bg210","kml","m32s0","n8m95"};
		DBApp.createTable("qi5i", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("qi5i"));
		String[] cols1 = {"fah","x212","li96","e9c30","h01zo","bapk1","zaq7"};
		DBApp.createTable("i27", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("i27"));
		String[] cols2 = {"rdj","dkf","de2","w4200","ob5","p39","af0w"};
		DBApp.createTable("xm", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("xm"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test125TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"e4pf"};
		DBApp.createTable("xq605", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("xq605"));
		String[] cols1 = {"di113","ai3","a18","k6s","krx3"};
		DBApp.createTable("xn0h", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("xn0h"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test126TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"b72n","daz"};
		DBApp.createTable("qmse", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("qmse"));
		String[] cols1 = {"ozk8","o9i","i54s1","fkkh","n2","wt","j9"};
		DBApp.createTable("vync", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("vync"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test127TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"ef7","c5","z170"};
		DBApp.createTable("i95i", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("i95i"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test128TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"l8nj","ul7m","si1","g1","z326","p5m7"};
		DBApp.createTable("jt800", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("jt800"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test129TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"q4","ek5w","zd8n","sff"};
		DBApp.createTable("aq652", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("aq652"));
		String[] cols1 = {"u3","s0c","hks","k40kn","b90","goy","iiz3","b725s","u2e","yb0"};
		DBApp.createTable("ja3v9", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("ja3v9"));
		String[] cols2 = {"zg424","d81","kp940","q60","fa","b05","n8","q4243","ok8c7","m87"};
		DBApp.createTable("xj302", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("xj302"));
		String[] cols3 = {"g1b","fq","k3c","e35","zz5","q8q2","ba9","uo9"};
		DBApp.createTable("ecxt4", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("ecxt4"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test130TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"z6d1i","vzbi9","gj7","y65w","iz55","n4o","j29"};
		DBApp.createTable("z47", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("z47"));
		String[] cols1 = {"v69m1","p78","h0"};
		DBApp.createTable("i2", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("i2"));
		String[] cols2 = {"bjs","d8g","pfk","gfnr5","ny3qw","t60v"};
		DBApp.createTable("br", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("br"));
		String[] cols3 = {"e5l53","sod","s631","b5q52"};
		DBApp.createTable("o03", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("o03"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test131TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"mqj","jfb8","x0"};
		DBApp.createTable("t7s0", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("t7s0"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test132TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"c3","y06","vol1","ih0b","vi883","nvd","ak","rr106"};
		DBApp.createTable("d0z0", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("d0z0"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test133TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"pe3","q0u","l5n4","e9","bc6","lqy","w2"};
		DBApp.createTable("y1", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("y1"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test134TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"jei"};
		DBApp.createTable("p5", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("p5"));
		String[] cols1 = {"tfy22","cm2e","eyb2x","rq3n","qor4","ygf9","i0ca","fnj"};
		DBApp.createTable("o75", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("o75"));
		String[] cols2 = {"ig1","g5v","w5","s1et","c1qs","u9","q70","gny","pdu"};
		DBApp.createTable("jcy", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("jcy"));
		String[] cols3 = {"hpcl1","ti","e69zi","h8","af","nowct","l1wwp","w9"};
		DBApp.createTable("a90", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("a90"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test135TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"uk5","jf","krn4","u4y","fluf9","u37","vh943"};
		DBApp.createTable("k53u1", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("k53u1"));
		String[] cols1 = {"zlc1","ldk","u89h","o8k0","wqu","isd","i43q"};
		DBApp.createTable("tj5", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("tj5"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test136TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"n5z","n829","r37","f8l","qyw","md","uh","ae0"};
		DBApp.createTable("e4", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("e4"));
		String[] cols1 = {"f42","l9","i88k","crn","l6kbc"};
		DBApp.createTable("wo", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("wo"));
		String[] cols2 = {"a86il","dna"};
		DBApp.createTable("rc", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("rc"));
		String[] cols3 = {"ynnw","z3","r5"};
		DBApp.createTable("jk", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("jk"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test137TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"ck7","n13ce","c0o3","fk3h","nc","c5y4j","k0l8"};
		DBApp.createTable("v3aj", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("v3aj"));
		String[] cols1 = {"k1p","w7","zym","w31","do","vyd","qh8q9"};
		DBApp.createTable("mv", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("mv"));
		String[] cols2 = {"cxh","xq","caz49","oz7","tch2","uv7h6","aopa","av0z","n37c"};
		DBApp.createTable("u5s7", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("u5s7"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test138TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"vii","s03","hf53r","u1xu","gis","e8s","c4u","b63l","db3","d9g9"};
		DBApp.createTable("m0y64", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("m0y64"));
		String[] cols1 = {"f2i","bo","c804","aatcc","o39","mq","h7802"};
		DBApp.createTable("jb5l2", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("jb5l2"));
		String[] cols2 = {"lak","vz","f2a6g","jq88","re9p"};
		DBApp.createTable("cba", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("cba"));
		String[] cols3 = {"xo1z9","vt0d","v32","iuy02"};
		DBApp.createTable("s5r", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("s5r"));
		String[] cols4 = {"gz","rcp","gb95"};
		DBApp.createTable("dm3", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("dm3"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test139TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"o08l","v0748","bp","r040k","url","fl","w4c"};
		DBApp.createTable("z5p", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("z5p"));
		String[] cols1 = {"ucmky","we447","k7"};
		DBApp.createTable("a55", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("a55"));
		String[] cols2 = {"em","fp","u3o","jf0","e9e","zc6h2"};
		DBApp.createTable("z39", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("z39"));
		String[] cols3 = {"ki5","cwa","rci","p41us","q7z"};
		DBApp.createTable("fac19", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("fac19"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test140TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"brm","owy","o4","fnaul","k4a","u018","bkh","is4bx","e5r"};
		DBApp.createTable("ii2", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ii2"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test141TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"x8","d70","s23y5","e41d5","y2","ccs","t7w","qep57","d28"};
		DBApp.createTable("ltngj", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ltngj"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test142TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"z7t","r3015","qv"};
		DBApp.createTable("ze9jp", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ze9jp"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test143TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"wudnj"};
		DBApp.createTable("jy2", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("jy2"));
		String[] cols1 = {"qd","wy5","fme"};
		DBApp.createTable("n1n7j", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("n1n7j"));
		String[] cols2 = {"re3l","z1h7","ffv1","w23mn","ct9","q2g","h51"};
		DBApp.createTable("p7", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("p7"));
		String[] cols3 = {"qg9v","b1","y98","df4bf","p96m","je173","b8","ck4","l35","xbs"};
		DBApp.createTable("q4qo", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("q4qo"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test144TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"siub","ta5","n5zwc","lj"};
		DBApp.createTable("fk6j0", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("fk6j0"));
		String[] cols1 = {"um7u"};
		DBApp.createTable("l92k", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("l92k"));
		String[] cols2 = {"ld","l6","y6","g9","g233","n2sc","kdj3","nx5"};
		DBApp.createTable("e07ob", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("e07ob"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test145TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"r3","de7x7","p11"};
		DBApp.createTable("xco", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("xco"));
		String[] cols1 = {"o7","jg5","y6n5","rn","d0","p71s","b155","zv","h52"};
		DBApp.createTable("d00", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("d00"));
		String[] cols2 = {"trs7"};
		DBApp.createTable("vvjcf", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("vvjcf"));
		String[] cols3 = {"fo2","hw8rg","ee3","qofae"};
		DBApp.createTable("dt", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("dt"));
		String[] cols4 = {"wzt9x","r1y","d0227"};
		DBApp.createTable("xld89", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("xld89"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test146TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"w9a","t9l","nns13","f1l","zk4","qh5","v911u"};
		DBApp.createTable("v2o9", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("v2o9"));
		String[] cols1 = {"o2","tye","p9ne0","yo9","gesn","j40","tn"};
		DBApp.createTable("ga", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("ga"));
		String[] cols2 = {"a2j5b","n264","b6q23","z0","a05","y2x"};
		DBApp.createTable("s19", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("s19"));
		String[] cols3 = {"cph","v3t","b1ad7","smm9"};
		DBApp.createTable("qpz", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("qpz"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test147TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"mv6","i718","i09","b2","k32","w2"};
		DBApp.createTable("hbf", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("hbf"));
		String[] cols1 = {"ayfr5","ai58","d06","jn7","l22"};
		DBApp.createTable("a95v", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("a95v"));
		String[] cols2 = {"lwv","c3"};
		DBApp.createTable("ua7f", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("ua7f"));
		String[] cols3 = {"abf"};
		DBApp.createTable("f6e0", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("f6e0"));
		String[] cols4 = {"ald","xj71","e4u","oz","n9","mvbqe"};
		DBApp.createTable("kz", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("kz"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test148TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"d0c","t647j","tzs6t"};
		DBApp.createTable("s4b", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("s4b"));
		String[] cols1 = {"dif65","mky","d86","g15","f285n","d221c","cw","n71","z4","h9ovs"};
		DBApp.createTable("k65", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("k65"));
		String[] cols2 = {"j0","e0e7r","g59yb","en6","o7","n7"};
		DBApp.createTable("l34", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("l34"));
		String[] cols3 = {"t7q"};
		DBApp.createTable("zna", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("zna"));
		String[] cols4 = {"kg","x27","mtx","orr","ub"};
		DBApp.createTable("i0xky", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("i0xky"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test149TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"ugg1x","mi4","t26","jj53","le8l1","yp3","e54k","s65"};
		DBApp.createTable("uq7", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("uq7"));
		String[] cols1 = {"c8","g4v65","if5y","m5","ru2","q7","cz","mdx0","f06","o1"};
		DBApp.createTable("te0pw", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("te0pw"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test150TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"ctd","u7d6"};
		DBApp.createTable("e6", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("e6"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test151TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"bjx6w","i9r9j","y1","o7b","wyx","j1","ek","w8h"};
		DBApp.createTable("mlc2", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("mlc2"));
		String[] cols1 = {"zc6","e9","pq8","acx7f","f7","hhb"};
		DBApp.createTable("v6", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("v6"));
		String[] cols2 = {"zs27","p4c","ub","o6l"};
		DBApp.createTable("rz1", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("rz1"));
		String[] cols3 = {"y7","m6p","y2i","ef9","i9avu","c3ya","vix","s359","p5y0h"};
		DBApp.createTable("k8j", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("k8j"));
		String[] cols4 = {"z7b","xk4rc","ccz"};
		DBApp.createTable("okv", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("okv"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test152TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"m37c","t2","gg0","gyb0p","ekn9","tf7","mm","zr2"};
		DBApp.createTable("aqf8h", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("aqf8h"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test153TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"k89y","v1sy","w8k","ko1","ene","vg246"};
		DBApp.createTable("xd", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("xd"));
		String[] cols1 = {"pfnm4","xz","r8","ys","zp"};
		DBApp.createTable("m9a", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("m9a"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test154TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"i7fkl","k7a","pmm8","yj6","tv8","n7","iq","va1","r3i"};
		DBApp.createTable("ih0", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("ih0"));
		String[] cols1 = {"ol1","vmr3v","xfx9","qdd"};
		DBApp.createTable("xbk85", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("xbk85"));
		String[] cols2 = {"vo2","f29","z0259","uxck","l26"};
		DBApp.createTable("abi", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("abi"));
		String[] cols3 = {"pz","us24e","xz"};
		DBApp.createTable("m6", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("m6"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test155TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"afr","tc962","vw0k","oz5","f94","djq4f","dia","xy5id","i7"};
		DBApp.createTable("w95", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("w95"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test156TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"w3","l7u32"};
		DBApp.createTable("pck2c", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("pck2c"));
		String[] cols1 = {"buoi","j4v","p7h","z41"};
		DBApp.createTable("eje", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("eje"));
		String[] cols2 = {"imx43","i21"};
		DBApp.createTable("dop", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("dop"));
		String[] cols3 = {"bxxr7","kh","tn535","es","ch99e","r2","heg33","v15"};
		DBApp.createTable("q8", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("q8"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test157TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"bfm","fp8r","i9c","l457f"};
		DBApp.createTable("njm", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("njm"));
		String[] cols1 = {"ss14","g2b","nz3","oc1","e7kg","zb"};
		DBApp.createTable("s1n", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("s1n"));
		String[] cols2 = {"zxlh","ebde","x3","lrq","o64","kf8","d6","mfsd","wzc","wlh09"};
		DBApp.createTable("lra", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("lra"));
		String[] cols3 = {"baz2"};
		DBApp.createTable("r04g4", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("r04g4"));
		String[] cols4 = {"cv","kf1t","qqv","h72","a8794","pp","v3q8"};
		DBApp.createTable("o839", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("o839"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test158TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"zje","g5e41"};
		DBApp.createTable("s6e2d", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("s6e2d"));
		String[] cols1 = {"i8r","aaa"};
		DBApp.createTable("b1y", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("b1y"));
		String[] cols2 = {"u7r"};
		DBApp.createTable("tc", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("tc"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test159TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"mkqy","g51","scjf","yao","hq8","e13q","bn055","tq85","u4b","i74"};
		DBApp.createTable("pyy", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("pyy"));
		String[] cols1 = {"hcn","i31","u66"};
		DBApp.createTable("k0", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("k0"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test160TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"u9246","btlb","t8","q3i","fw3","w3d","kbh","zngpe","zhz","wl7"};
		DBApp.createTable("mp42", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("mp42"));
		String[] cols1 = {"pa4","f436","dj","ja0s","a0w","d3","o057","j284"};
		DBApp.createTable("r0r", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("r0r"));
		String[] cols2 = {"e4d","wogq","og3q","l93","wm9"};
		DBApp.createTable("p38", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("p38"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test161TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"fbh09","wt","mwe"};
		DBApp.createTable("b8v", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("b8v"));
		String[] cols1 = {"hx1","mx7","b5e","tpo43","pa65","f8c"};
		DBApp.createTable("e1kr", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("e1kr"));
		String[] cols2 = {"se1","d0m","tmf"};
		DBApp.createTable("qhk", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("qhk"));
		String[] cols3 = {"p2","ry6","ar","q9i47","k2s0c","l5","qo96p","jf4y1","bus6"};
		DBApp.createTable("wg66", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("wg66"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test162TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"p4n","yl","fa8","ms6w","e9j","twgw"};
		DBApp.createTable("lh8", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("lh8"));
		String[] cols1 = {"j9r","w8","qpb0x"};
		DBApp.createTable("hpy", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("hpy"));
		String[] cols2 = {"e0vb2","ay7","e53","e3","jp0","d472","g2","gi29","pc"};
		DBApp.createTable("cb", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("cb"));
		String[] cols3 = {"t212","q4p"};
		DBApp.createTable("ece", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("ece"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test163TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"ffc6","f0e","ilu1q","x44","ju","nq2x"};
		DBApp.createTable("l1", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("l1"));
		String[] cols1 = {"x7p07","f4"};
		DBApp.createTable("m724", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("m724"));
		String[] cols2 = {"vmqm","q04fq"};
		DBApp.createTable("i5", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("i5"));
		String[] cols3 = {"zr13","m1u","m03f","uxy","og","orrsl","wk1"};
		DBApp.createTable("p2v5", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("p2v5"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test164TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"qd","w342v","tne","rg9xk","q2"};
		DBApp.createTable("a2", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("a2"));
		String[] cols1 = {"e7m","zgp","ibu5a","l84","qer0p","si4","a6x","q03","fs374","rx"};
		DBApp.createTable("nt3p", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("nt3p"));
		String[] cols2 = {"j73g","ft1","oj2","qtvm","w3f","qj","jq"};
		DBApp.createTable("vr4", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("vr4"));
		String[] cols3 = {"dx6","v3","rj","cc","yf","vu"};
		DBApp.createTable("gqf25", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("gqf25"));
		String[] cols4 = {"mg70s","iy72t","jm5","j6v","s7","r90yd","xw","lfh"};
		DBApp.createTable("ih94", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("ih94"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test165TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"gwp","uv21","nbi","q1w"};
		DBApp.createTable("s88", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("s88"));
		String[] cols1 = {"h62z3"};
		DBApp.createTable("ziu", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("ziu"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test166TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"ro07","f2","c26","r8v","c2","yrq24","f90x2"};
		DBApp.createTable("if076", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("if076"));
		String[] cols1 = {"w6","y4n","axk","lwo","hutc6","ga","f89","h5d","vkf4h"};
		DBApp.createTable("ft17", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("ft17"));
		String[] cols2 = {"pn4","z11x"};
		DBApp.createTable("nk8", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("nk8"));
		String[] cols3 = {"dr7","r6v","yoq","lxw52","c23","sc","i27","t5c4"};
		DBApp.createTable("q06", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("q06"));
		String[] cols4 = {"mej","x6372"};
		DBApp.createTable("o5m4", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("o5m4"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test167TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"esj","y5m","yv39","ss"};
		DBApp.createTable("d706", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("d706"));
		String[] cols1 = {"c7k","f37f","zakz","q9","lq","bh157"};
		DBApp.createTable("a84", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("a84"));
		String[] cols2 = {"g1495","ur6t","an0","xbsye","q570","mf3y","qj1","m0","p46cq"};
		DBApp.createTable("jx8y", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("jx8y"));
		String[] cols3 = {"mp8"};
		DBApp.createTable("k3b", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("k3b"));
		String[] cols4 = {"dl7","y149k","l3jl","glb","x4t"};
		DBApp.createTable("cec", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("cec"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test168TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"rvd9","m8","d3b2","k88","ny","ld3","in8"};
		DBApp.createTable("wvh", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("wvh"));
		String[] cols1 = {"k5a","x4","jk91","mtm5","z974","qryj"};
		DBApp.createTable("fy", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("fy"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test169TableFolderCreationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"g01","d8","gd11","rf8","j4","z2qc","q85","dnq","oor","x7136"};
		DBApp.createTable("b5", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace0.contains("b5"));
		String[] cols1 = {"g068","bf0","k3","z8jwk","e21","c10","bii","u0w01","h3","x89"};
		DBApp.createTable("uwz14", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace1.contains("uwz14"));
		String[] cols2 = {"ia5k","q64u"};
		DBApp.createTable("dl2", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace2.contains("dl2"));
		String[] cols3 = {"c17","a77vt","z19","zg7","kr48","zve","rjm83","df5"};
		DBApp.createTable("i090", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace3.contains("i090"));
		String[] cols4 = {"t2f30","gab","f79","x29t","m6zyz","a6gt"};
		DBApp.createTable("upmkm", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A folder should be created for each table.", fileManagerTrace4.contains("upmkm"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test0TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"a37c","yzl","yp","x40","i58","c341l","i41","p2"};
		DBApp.createTable("xq", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("xq.db"));
		String[] cols1 = {"gxr","c2m4d","xf2","ai83d","u1h","c5g","i2us","v1je2"};
		DBApp.createTable("k3t51", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("k3t51.db"));
		String[] cols2 = {"x91","mr7","hs6","qa8m4","s84zk","x3635","cu7tm","is2","w2"};
		DBApp.createTable("c8r", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("c8r.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test1TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"s09","t21","p809","f29","p7d","ne4","o6f","b247","h2v","agy"};
		DBApp.createTable("wl", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("wl.db"));
		String[] cols1 = {"o8k","s63","o01","n2","fay"};
		DBApp.createTable("dr2", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("dr2.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test2TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"sg","v2fjy","xwt","u0","t26r7","ao","shthv","c79e"};
		DBApp.createTable("a27rr", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("a27rr.db"));
		String[] cols1 = {"fit","p4m9","p0w","c4iw1","z8e"};
		DBApp.createTable("g8", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("g8.db"));
		String[] cols2 = {"p23","esd9"};
		DBApp.createTable("ff8l", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("ff8l.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test3TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"xp37","k93","rbazc","xn","i63fs","h272","hxj4","j72x","e559p"};
		DBApp.createTable("ne8t8", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("ne8t8.db"));
		String[] cols1 = {"kx7","i3aci","ghyd7","h1u9","di2p","mdtw","hlq","p24wn"};
		DBApp.createTable("vt", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("vt.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test4TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"ves","n74v","h54z6"};
		DBApp.createTable("kz", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("kz.db"));
		String[] cols1 = {"wi7"};
		DBApp.createTable("ur9l8", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("ur9l8.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test5TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"y01","x29","hw5","d6r1t","g766","yq99","j8lc6","kj3"};
		DBApp.createTable("f4z", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("f4z.db"));
		String[] cols1 = {"o7d","ww3r"};
		DBApp.createTable("sy17q", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("sy17q.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test6TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"f21pw","qm98","a5ul","q4627","jol","s63j4","b8cth","xd7cj","g300g","ua"};
		DBApp.createTable("sa40r", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("sa40r.db"));
		String[] cols1 = {"u34","wei","zy"};
		DBApp.createTable("a73", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("a73.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test7TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"i98","sg3u","n3fz","y7o","z8"};
		DBApp.createTable("d2t", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("d2t.db"));
		String[] cols1 = {"cba","o4","ftjv8","qx","sp","lz5k6","p3dc"};
		DBApp.createTable("b20", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("b20.db"));
		String[] cols2 = {"l4sj"};
		DBApp.createTable("tf", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("tf.db"));
		String[] cols3 = {"vc1","u2222","t5g","qb6","xxo","sms1","ei9","tx0","c50"};
		DBApp.createTable("wq012", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("wq012.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test8TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"d7","c7o"};
		DBApp.createTable("d8", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("d8.db"));
		String[] cols1 = {"ca","p1p9i","jh9i7","p60","j8kul","lt09y","xf4yl"};
		DBApp.createTable("c4x", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("c4x.db"));
		String[] cols2 = {"spz5","d3"};
		DBApp.createTable("i06j", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("i06j.db"));
		String[] cols3 = {"hj","y92","ek6","vc97q","y2","aj","o5w","a9l3","w6","on2n3"};
		DBApp.createTable("jl6", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("jl6.db"));
		String[] cols4 = {"cxg26","h41e","ct6","bh","q8qq","p5fn9","x1","at5","gk"};
		DBApp.createTable("cku", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("cku.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test9TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"p01l","fz8","z7xi1","j3","uev","iq3","tnvj","afy","i6113","qb"};
		DBApp.createTable("xlux", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("xlux.db"));
		String[] cols1 = {"qib","aw4","r43","af","wjy","bng","jh","nzkui","fto"};
		DBApp.createTable("if4vf", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("if4vf.db"));
		String[] cols2 = {"u5t","t4m4","kf8"};
		DBApp.createTable("g4", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("g4.db"));
		String[] cols3 = {"wkhro","u7h","a89","o7df","y66r","vz90","ku9","p6","o99"};
		DBApp.createTable("z4w", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("z4w.db"));
		String[] cols4 = {"m506","t4","aqcjg","e14"};
		DBApp.createTable("dfsa8", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("dfsa8.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test10TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"jm60","qn5l8","c5"};
		DBApp.createTable("u5r", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("u5r.db"));
		String[] cols1 = {"rkm8","kj","l1h","l3t","usnv4","fe7b8","b2i","q8mkl"};
		DBApp.createTable("oe6", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("oe6.db"));
		String[] cols2 = {"r092"};
		DBApp.createTable("fm5", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("fm5.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test11TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"i25"};
		DBApp.createTable("mh", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("mh.db"));
		String[] cols1 = {"mv834"};
		DBApp.createTable("rsf1u", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("rsf1u.db"));
		String[] cols2 = {"p0z","m90f9","l9137","y197"};
		DBApp.createTable("me5", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("me5.db"));
		String[] cols3 = {"o986f","g1u","g4","lwl","zycrx","n8x","w04"};
		DBApp.createTable("ao5h", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("ao5h.db"));
		String[] cols4 = {"d91","gr7l6","fvj","b1i","bx"};
		DBApp.createTable("j6m", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("j6m.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test12TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"pu"};
		DBApp.createTable("n07", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("n07.db"));
		String[] cols1 = {"gmm","f9m2a","m22w9","p4c8","c1","z79o","v5","s0l","vb2"};
		DBApp.createTable("r9", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("r9.db"));
		String[] cols2 = {"qe"};
		DBApp.createTable("u8d2b", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("u8d2b.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test13TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"ma5e3","owf","cs","r6i","ry0a","zj94","yook","e5e7","wk5s3","x9k0"};
		DBApp.createTable("a6", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("a6.db"));
		String[] cols1 = {"u7uu4","yixn2","n7c","r6w","b6qa","s81","z77","i3t85","b8y","d10a"};
		DBApp.createTable("q3d", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("q3d.db"));
		String[] cols2 = {"fk","fr7","v46","xo","f6","h0hy","m2044","s2a","qn8"};
		DBApp.createTable("q29u", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("q29u.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test14TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"yf","ov8","byn","hop6d","l45a8"};
		DBApp.createTable("go4zn", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("go4zn.db"));
		String[] cols1 = {"h43b"};
		DBApp.createTable("o59", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("o59.db"));
		String[] cols2 = {"q3k","b1e4","t53","vo","tw8"};
		DBApp.createTable("m0", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("m0.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test15TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"w0bh","r99","x34","hw5","xtuh"};
		DBApp.createTable("z78", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("z78.db"));
		String[] cols1 = {"ujx3w","f11b3","r0","y7u","b0i8s","lx73t","k81","zt","su"};
		DBApp.createTable("xl0", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("xl0.db"));
		String[] cols2 = {"z50","x6"};
		DBApp.createTable("z9r", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("z9r.db"));
		String[] cols3 = {"iz","dq8","g251","n83"};
		DBApp.createTable("t8n", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("t8n.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test16TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"h23","i33d"};
		DBApp.createTable("klu", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("klu.db"));
		String[] cols1 = {"phvz9","ty","g57q","m09","u17"};
		DBApp.createTable("f5g", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("f5g.db"));
		String[] cols2 = {"i8","zr"};
		DBApp.createTable("xp6y", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("xp6y.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test17TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"kcc","iisrf","dmi","yao","em7","h45p","jmc","k2"};
		DBApp.createTable("kdwqf", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("kdwqf.db"));
		String[] cols1 = {"c73","opv"};
		DBApp.createTable("ye5uk", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("ye5uk.db"));
		String[] cols2 = {"gfv9","tu5o5","tw8g","z0","s8m","r0952","c7"};
		DBApp.createTable("h997", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("h997.db"));
		String[] cols3 = {"m8","xd7p"};
		DBApp.createTable("qsl2a", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("qsl2a.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test18TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"m74z8","nu5t","ho","lrsdu","noz","y4f","i4r","z4l","p9l","e4xp"};
		DBApp.createTable("gg", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("gg.db"));
		String[] cols1 = {"w7","qo","ha5","y95","i9","m5","jb24","wa8"};
		DBApp.createTable("ww2", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("ww2.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test19TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"ez","rp9","gx1","ag","nod"};
		DBApp.createTable("g5j54", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("g5j54.db"));
		String[] cols1 = {"kfs","xe8","xfn50","k94","opfvm","klmvh","w8dc","vrb"};
		DBApp.createTable("lb007", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("lb007.db"));
		String[] cols2 = {"pp","ekc18","l5r3","azh","gyb9v","tawn","h0j47","f06","d39","h2o"};
		DBApp.createTable("a38", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("a38.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test20TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"q214f","bl0","y22","zzb90","xz272","d367"};
		DBApp.createTable("y8i0f", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("y8i0f.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test21TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"b01","yr7","wg7","mwh0"};
		DBApp.createTable("jbs82", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("jbs82.db"));
		String[] cols1 = {"vo4","ww4","k4","fxjh","zne","k9oa2","a94"};
		DBApp.createTable("z3d", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("z3d.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test22TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"ub8","n4","dawj8","n774i","ilo45","fl","svw","wj85","uh"};
		DBApp.createTable("r7934", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("r7934.db"));
		String[] cols1 = {"q5m","h0","g7q"};
		DBApp.createTable("y7s", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("y7s.db"));
		String[] cols2 = {"t0cp7","iz","fl8","ph","alu","i2s","v90"};
		DBApp.createTable("mz", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("mz.db"));
		String[] cols3 = {"h9","dp7","k562y","a3","rrie","sdq","p1","w94k9"};
		DBApp.createTable("b06", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("b06.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test23TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"dhg"};
		DBApp.createTable("m24", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("m24.db"));
		String[] cols1 = {"q8oor","c3cms","l3a6","geww7","w6l","r8917","vb9","p85","yvd8"};
		DBApp.createTable("c16", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("c16.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test24TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"oc65","hej","im","t28","w8","sk8","kw3cy","v377","t53","y1"};
		DBApp.createTable("yw1i4", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("yw1i4.db"));
		String[] cols1 = {"rq4eo","hn7","ac","u7q5","r6y"};
		DBApp.createTable("bj5qi", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("bj5qi.db"));
		String[] cols2 = {"rez68","fou","dsz5q","r668m","py0a","pen"};
		DBApp.createTable("g26og", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("g26og.db"));
		String[] cols3 = {"u5v5i","v66"};
		DBApp.createTable("nx", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("nx.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test25TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"f3r","zrfa5","bdlu","hl6","dr","u15"};
		DBApp.createTable("ir78", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("ir78.db"));
		String[] cols1 = {"v6o","h7l","wl89","j14"};
		DBApp.createTable("rn8", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("rn8.db"));
		String[] cols2 = {"x85pu","qou0","r0","ep75","f1"};
		DBApp.createTable("i1", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("i1.db"));
		String[] cols3 = {"xkus","ug4"};
		DBApp.createTable("bc97u", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("bc97u.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test26TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"s9b","y19","nt89q"};
		DBApp.createTable("bg36", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("bg36.db"));
		String[] cols1 = {"b617","gr9","p0a","z1rk","nhv","q3","zd1","zohr0","d1785"};
		DBApp.createTable("h6f", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("h6f.db"));
		String[] cols2 = {"mo60z","et0u","b8","im0","cg72","k44","k82","iyvh","mvb"};
		DBApp.createTable("nt01", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("nt01.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test27TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"j5olo","r2w","ue7j2","e94"};
		DBApp.createTable("yci1w", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("yci1w.db"));
		String[] cols1 = {"x9t","b93","woa2r","rf","n26","j964h","j7i","jjt6p","o2i"};
		DBApp.createTable("v71", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("v71.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test28TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"v282","te631","as8","d09","v44","g2g47","d41","q1w"};
		DBApp.createTable("laz5x", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("laz5x.db"));
		String[] cols1 = {"ny05f","tm","wzn"};
		DBApp.createTable("qwpj4", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("qwpj4.db"));
		String[] cols2 = {"pl","sjtij","y86b0","d66","hq7","b6","yw","wet","c3k","l864"};
		DBApp.createTable("r4od3", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("r4od3.db"));
		String[] cols3 = {"d2v","a22","g7","j5q","ngxc","i32","qex","x11","c5e"};
		DBApp.createTable("s2o", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("s2o.db"));
		String[] cols4 = {"w2es2","x4"};
		DBApp.createTable("bl7", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("bl7.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test29TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"e05","wew","e87","nd","dt0v","qdj","o2h","pj0n0","bjj"};
		DBApp.createTable("dqea", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("dqea.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test30TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"g4d5","re6c","v2","zg","i5q","qje","wa5"};
		DBApp.createTable("e07", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("e07.db"));
		String[] cols1 = {"a8okb"};
		DBApp.createTable("th", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("th.db"));
		String[] cols2 = {"oh","ha","k81","jr"};
		DBApp.createTable("f25l", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("f25l.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test31TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"gu884","e00l","m5x","bml85","w4v","dgu80","vw9","pm9"};
		DBApp.createTable("hu", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("hu.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test32TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"p90","r34"};
		DBApp.createTable("j06tm", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("j06tm.db"));
		String[] cols1 = {"y46","mrn","c5gq","q2h","xeog","bi","rs086","px","fxz","ar7rv"};
		DBApp.createTable("l0", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("l0.db"));
		String[] cols2 = {"kq","tnbi2"};
		DBApp.createTable("d258", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("d258.db"));
		String[] cols3 = {"td"};
		DBApp.createTable("n2kls", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("n2kls.db"));
		String[] cols4 = {"su5w","p06z1"};
		DBApp.createTable("o33p7", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("o33p7.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test33TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"jt5"};
		DBApp.createTable("ca4dj", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("ca4dj.db"));
		String[] cols1 = {"q49","bi5","pbqj","l7","o75","k08z9","v321","gy"};
		DBApp.createTable("izd", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("izd.db"));
		String[] cols2 = {"r4y73","a126o"};
		DBApp.createTable("i8n44", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("i8n44.db"));
		String[] cols3 = {"q3","gco3c","k3hh8","j6hv4","hjz8","jem96"};
		DBApp.createTable("vb9", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("vb9.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test34TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"g3e","um3","b4k","g3h"};
		DBApp.createTable("p6", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("p6.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test35TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"koz","txpkk"};
		DBApp.createTable("h82", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("h82.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test36TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"t7m0","h26","s00","rnv","pz9wo","m03hs","kcz","m7"};
		DBApp.createTable("vhm", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("vhm.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test37TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"vo","ne13","y72","lz6","ymi3"};
		DBApp.createTable("wgu8h", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("wgu8h.db"));
		String[] cols1 = {"b7p9","h5r","m6wy9","c3u","rf8u2","xt5","s40","z24","j2v","d2"};
		DBApp.createTable("xp77", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("xp77.db"));
		String[] cols2 = {"ef8","dwn","pfbvb"};
		DBApp.createTable("bba", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("bba.db"));
		String[] cols3 = {"u8","r8u","g8v","l05n6","ssy","sq","c37k4","f1a80"};
		DBApp.createTable("vxf", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("vxf.db"));
		String[] cols4 = {"j8","xkf","f69","d053","ti","p1","i124"};
		DBApp.createTable("huc", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("huc.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test38TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"l3u6","v0ydj","bs3","eq9","fvs","f1u3"};
		DBApp.createTable("iha", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("iha.db"));
		String[] cols1 = {"xidtz"};
		DBApp.createTable("x3", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("x3.db"));
		String[] cols2 = {"v4kb","rjkj","qgd15","wxu7","in2vw","p1c1u"};
		DBApp.createTable("x8q8s", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("x8q8s.db"));
		String[] cols3 = {"m90g","t02jy","kn70","l2i","x36"};
		DBApp.createTable("sj9", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("sj9.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test39TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"sde","be","kky","m2","sib"};
		DBApp.createTable("k2q", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("k2q.db"));
		String[] cols1 = {"s19g9"};
		DBApp.createTable("f7w", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("f7w.db"));
		String[] cols2 = {"w49o"};
		DBApp.createTable("t20l4", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("t20l4.db"));
		String[] cols3 = {"dgd6","wk","x83","ux","aon","d324s","wlp1s","r6qci","eur","o736e"};
		DBApp.createTable("gwo", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("gwo.db"));
		String[] cols4 = {"toj","b4","h51","z8x","yyu","jk7","ai4h4","d84","ry8bp","ax77h"};
		DBApp.createTable("v92", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("v92.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test40TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"o7","p4a","zl","eo5","owix","n9v"};
		DBApp.createTable("ik8ab", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("ik8ab.db"));
		String[] cols1 = {"tlp4c","yv","v71b","qf8","lgq","rx","n8qi"};
		DBApp.createTable("zkb", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("zkb.db"));
		String[] cols2 = {"lnm","p91w","e5b0p","b88","pnl","l64"};
		DBApp.createTable("do", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("do.db"));
		String[] cols3 = {"m2","uf","wl1yw","yr61","z8k5","zeolg"};
		DBApp.createTable("lo0xk", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("lo0xk.db"));
		String[] cols4 = {"ytfob","x5o","ex10b","mzt","h7q8","eewa","ds0"};
		DBApp.createTable("u5d", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("u5d.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test41TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"rb3","l4f","j94h4","l23","a584"};
		DBApp.createTable("m1", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("m1.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test42TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"h01"};
		DBApp.createTable("o90", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("o90.db"));
		String[] cols1 = {"mjf5","o86","tj"};
		DBApp.createTable("x153j", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("x153j.db"));
		String[] cols2 = {"lc71x","s311","i718e","a7"};
		DBApp.createTable("sq", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("sq.db"));
		String[] cols3 = {"d0lc","sb9","oz8","s8","qh7","mzg","u4x","s85"};
		DBApp.createTable("c2j", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("c2j.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test43TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"oy","p45"};
		DBApp.createTable("e1nj8", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("e1nj8.db"));
		String[] cols1 = {"p9g44","lo5","bnxd","o3i","eo36n","h6","j9","w69","kn7"};
		DBApp.createTable("z1", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("z1.db"));
		String[] cols2 = {"as4","evr6","z9x","n8989","zva"};
		DBApp.createTable("rfs", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("rfs.db"));
		String[] cols3 = {"ha1","p06zh"};
		DBApp.createTable("ob2", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("ob2.db"));
		String[] cols4 = {"o0","n1brs","i9m0","x98","dmj","y8o","o7lp3"};
		DBApp.createTable("hcila", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("hcila.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test44TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"l0j5","hqmh8","zs0s"};
		DBApp.createTable("whv2w", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("whv2w.db"));
		String[] cols1 = {"p7372","x427a","t0z","nd3","g1v","yls"};
		DBApp.createTable("a5", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("a5.db"));
		String[] cols2 = {"h0","a1","f63","vlt","c74","j6","pql7j","auc4"};
		DBApp.createTable("qk4", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("qk4.db"));
		String[] cols3 = {"h6","ig","bov","en","rl40","jnf","bb","zx6l"};
		DBApp.createTable("zu", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("zu.db"));
		String[] cols4 = {"jye","xy4","u0i8w","z96se","xi","r83z","q59","cr6","rh"};
		DBApp.createTable("vh1e", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("vh1e.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test45TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"se3xw","k93","c5ps","t09","h50","kl3","o6qs","e9ad"};
		DBApp.createTable("ed", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("ed.db"));
		String[] cols1 = {"r51","jq","qq0t2","xa9","wrd"};
		DBApp.createTable("h6519", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("h6519.db"));
		String[] cols2 = {"n3lw"};
		DBApp.createTable("l8", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("l8.db"));
		String[] cols3 = {"a6f","ri0r0","h12","wm","wlo02","s5h46","x9g","j783","nq7g","dcan2"};
		DBApp.createTable("o3s9p", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("o3s9p.db"));
		String[] cols4 = {"rtb","czbk","b2j","og3","d6uy","s9ss","xn9","ah19","xwm","yb9en"};
		DBApp.createTable("v9b9j", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("v9b9j.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test46TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"iaow","az6","wqbm","s6i","sp6b","i77","umna"};
		DBApp.createTable("s6", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("s6.db"));
		String[] cols1 = {"k8","k0276","k028","zcl","q8"};
		DBApp.createTable("o7", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("o7.db"));
		String[] cols2 = {"b22","vn39","c43n","q5","m41","ml13h","y5b","j6sk","w3r"};
		DBApp.createTable("i60", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("i60.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test47TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"t93","o12w1","j61i5","zv9cn"};
		DBApp.createTable("t84", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("t84.db"));
		String[] cols1 = {"o5e2","m936","m528","pj4e"};
		DBApp.createTable("kni72", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("kni72.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test48TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"c08e"};
		DBApp.createTable("p51", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("p51.db"));
		String[] cols1 = {"a29u","er3","pg","tz66","p00","lfy"};
		DBApp.createTable("y16", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("y16.db"));
		String[] cols2 = {"lc","hu6","ls","x03","k1q","v4uv","aq5","dmti","a15"};
		DBApp.createTable("sw5", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("sw5.db"));
		String[] cols3 = {"qp","f621","l0r","z42","d7h","nf2","btg9e","men76","x934"};
		DBApp.createTable("m3hl", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("m3hl.db"));
		String[] cols4 = {"nen6","x3i","o46cw"};
		DBApp.createTable("dz", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("dz.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test49TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"f73","qe9"};
		DBApp.createTable("x39", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("x39.db"));
		String[] cols1 = {"x16","g1t9","fm78"};
		DBApp.createTable("u0q9p", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("u0q9p.db"));
		String[] cols2 = {"d8eb","g1z3","j40y5","pz","m3k"};
		DBApp.createTable("qel37", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("qel37.db"));
		String[] cols3 = {"akl","tnm","vh7vr","wi","l1","z41","ml2","p5","o5"};
		DBApp.createTable("q99", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("q99.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test50TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"sc","f8","vov12","te4","cs08t","vb6od","b88","h09","t7q2"};
		DBApp.createTable("m55", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("m55.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test51TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"e9i2"};
		DBApp.createTable("h6p5", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("h6p5.db"));
		String[] cols1 = {"jl4"};
		DBApp.createTable("je4", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("je4.db"));
		String[] cols2 = {"j67c","q0","k411","dy7","t93","o575","ug","bz0b","ex"};
		DBApp.createTable("u4ua", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("u4ua.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test52TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"zz3","w6y"};
		DBApp.createTable("a3j", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("a3j.db"));
		String[] cols1 = {"yz81","n8qih","iyy","dk","v2tu","ggf"};
		DBApp.createTable("e7", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("e7.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test53TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"atkev","l7","an50","cky"};
		DBApp.createTable("e86p", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("e86p.db"));
		String[] cols1 = {"n0p"};
		DBApp.createTable("x67fl", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("x67fl.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test54TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"f1sb","niu","g90ni","vnb","l1e60","sts2","tpy","l7te","ilmm"};
		DBApp.createTable("azdjm", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("azdjm.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test55TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"qln","xq","br","se5","l58u5","srt"};
		DBApp.createTable("k2u78", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("k2u78.db"));
		String[] cols1 = {"hbs","r45","ajga4","iq6","vn9"};
		DBApp.createTable("ey6ic", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("ey6ic.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test56TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"cd85","r6","f1","s8","xv60"};
		DBApp.createTable("oq8ed", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("oq8ed.db"));
		String[] cols1 = {"mm7"};
		DBApp.createTable("k6", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("k6.db"));
		String[] cols2 = {"r1f","w57","c34j","z95x","d8c","gm"};
		DBApp.createTable("dk", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("dk.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test57TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"w9239","fl3","zun2","be78","u32o","sx","gd752","on","ja7"};
		DBApp.createTable("e8", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("e8.db"));
		String[] cols1 = {"a23v","g0h","i0j"};
		DBApp.createTable("b0x", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("b0x.db"));
		String[] cols2 = {"inn7","cc8","qc","eg","p7","lwu","jr2","tf"};
		DBApp.createTable("w9i", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("w9i.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test58TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"o2q1y","x1v00","t10z4"};
		DBApp.createTable("g3", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("g3.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test59TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"gm0i0","f9h","vx","x82","p0u6","jsb","sp","occ","i4"};
		DBApp.createTable("r6m", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("r6m.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test60TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"rf17","q52hd","a2pn","z1l49","fzk","pylmo","n9","tc2"};
		DBApp.createTable("lol", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("lol.db"));
		String[] cols1 = {"l833o","uo109","yi1","zt","t5414","h1g8k"};
		DBApp.createTable("qh0v", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("qh0v.db"));
		String[] cols2 = {"l2s","w3q","f92","m56","y0i"};
		DBApp.createTable("n9kg3", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("n9kg3.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test61TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"gli8","om7","qxh2i","o13"};
		DBApp.createTable("mp77a", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("mp77a.db"));
		String[] cols1 = {"oyh","ji0ai","gr","on3","a8","des"};
		DBApp.createTable("y597v", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("y597v.db"));
		String[] cols2 = {"k27z7"};
		DBApp.createTable("p5", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("p5.db"));
		String[] cols3 = {"dt5","p1qh","o0ub6","tphr4","u5t"};
		DBApp.createTable("jc6s6", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("jc6s6.db"));
		String[] cols4 = {"oi4","ua","vi7","g0","h82","gfv","a28","eiyaw"};
		DBApp.createTable("a4", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("a4.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test62TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"q9s","r2","v4","d30","p8vr"};
		DBApp.createTable("zk4", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("zk4.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test63TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"lpl07","vkwlf","b7","nn18r","k3t","b6","v5htk","h6nr","u6","ri5t5"};
		DBApp.createTable("yxj8s", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("yxj8s.db"));
		String[] cols1 = {"hr","j51","o68","oa","b3h","jv","man2","j00j"};
		DBApp.createTable("wxipj", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("wxipj.db"));
		String[] cols2 = {"egqv","e9","qx3","ad","qpy2","j5","pp","ia"};
		DBApp.createTable("j9", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("j9.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test64TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"t7og0","j42r","otoq"};
		DBApp.createTable("lvb29", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("lvb29.db"));
		String[] cols1 = {"l5"};
		DBApp.createTable("i91", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("i91.db"));
		String[] cols2 = {"tn5","q79u4","p99","v65","w753","pm76r","rvi","s25"};
		DBApp.createTable("a6l", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("a6l.db"));
		String[] cols3 = {"csj","dwn","q9z","b2f6n","y644","w79","gt9","m5102","qr5","la4"};
		DBApp.createTable("s12ba", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("s12ba.db"));
		String[] cols4 = {"q189","qls","z65"};
		DBApp.createTable("r76", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("r76.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test65TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"f6i","bfc","rt3"};
		DBApp.createTable("dv4h", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("dv4h.db"));
		String[] cols1 = {"clr","umh","o75yb","j42","m3t","l4284","q786x"};
		DBApp.createTable("b985", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("b985.db"));
		String[] cols2 = {"ji38","g5a","n2","z0g","q4j5","w33","jn4","cb9","s7m"};
		DBApp.createTable("rit", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("rit.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test66TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"y2","h63"};
		DBApp.createTable("pl6", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("pl6.db"));
		String[] cols1 = {"b4d","v53","lp5"};
		DBApp.createTable("xrjx5", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("xrjx5.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test67TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"lw7k","j2b85","f3k","k9i6","f3","c1s0c"};
		DBApp.createTable("v2243", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("v2243.db"));
		String[] cols1 = {"to9","q1","gk","iiy8x","yxf","kf37"};
		DBApp.createTable("mvi", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("mvi.db"));
		String[] cols2 = {"f0","e8","sj","o9e","p2x","oc48","ncf","rte","sd","lu"};
		DBApp.createTable("nehd", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("nehd.db"));
		String[] cols3 = {"zsq","z4v6","xl6","wp","p1y","q2sv","ojt95","i8","od","badb"};
		DBApp.createTable("t7", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("t7.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test68TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"j1","c24ty","pr","h695b","x3","x08v","e5","l8t8","n8","y8n"};
		DBApp.createTable("y7y5", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("y7y5.db"));
		String[] cols1 = {"iq7ot","jp5","k1t","ae9","u54"};
		DBApp.createTable("r12", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("r12.db"));
		String[] cols2 = {"qo3","aa4","oq","vm4","vsu1w","a1","us7","d5ai","f0d","bsb"};
		DBApp.createTable("rytw", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("rytw.db"));
		String[] cols3 = {"m0","rk","pz"};
		DBApp.createTable("v1520", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("v1520.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test69TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"a8","lj4h","i1","xt5p","uuy","aq0"};
		DBApp.createTable("mbhw", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("mbhw.db"));
		String[] cols1 = {"oe7","lg","n1s5m","fcw","kr","p0q6m","rge","auo9","m9","kj1"};
		DBApp.createTable("g8", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("g8.db"));
		String[] cols2 = {"td"};
		DBApp.createTable("vf40", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("vf40.db"));
		String[] cols3 = {"e01q7","nff","s0r9","w0k1z","z1wp","a2o","y49","o3","ig","o91"};
		DBApp.createTable("h0v", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("h0v.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test70TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"uc","n5","s1","i59","aq0","b9e"};
		DBApp.createTable("kad2l", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("kad2l.db"));
		String[] cols1 = {"lv1","wrp5w","v9r9","maw3","zz","le","k7ql6","x3i"};
		DBApp.createTable("sgp", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("sgp.db"));
		String[] cols2 = {"z32ga","m7f","jf0","yw964","m035","r8","v9","u46","e7sa"};
		DBApp.createTable("zlc", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("zlc.db"));
		String[] cols3 = {"qyo5w","a6","b09","p6","lpi9","i8n96","q6g4","q1k"};
		DBApp.createTable("k7m", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("k7m.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test71TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"uv5","f8tw","q1yg","w676","dcm","wyi","h2p","bqw"};
		DBApp.createTable("zz", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("zz.db"));
		String[] cols1 = {"b9a","bgz","ac5j","sh228"};
		DBApp.createTable("qm", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("qm.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test72TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"egpm0"};
		DBApp.createTable("f21", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("f21.db"));
		String[] cols1 = {"h7i","ah4","rr4"};
		DBApp.createTable("h6z5", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("h6z5.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test73TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"k1mln","kd","yh","zi1","wvpwe","mj","a1c","st4","d02p5"};
		DBApp.createTable("b6t7y", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("b6t7y.db"));
		String[] cols1 = {"ri36","tgz0","fe15","i53"};
		DBApp.createTable("irf8", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("irf8.db"));
		String[] cols2 = {"j04","r3d","wnn2d","s0","ej0","ke83"};
		DBApp.createTable("r5", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("r5.db"));
		String[] cols3 = {"n8a","a996o","b0h"};
		DBApp.createTable("t2", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("t2.db"));
		String[] cols4 = {"m2d","p76","pd1t","c3ar"};
		DBApp.createTable("h6", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("h6.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test74TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"ma8","f0qz","rxe","c0ol4","u8","qz9sh","v28","sa6p3","x3j","ov5"};
		DBApp.createTable("m4h", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("m4h.db"));
		String[] cols1 = {"h6gx","i1aln","o0","goo"};
		DBApp.createTable("i2v", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("i2v.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test75TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"ubw4","tt","z25k","jd08s","nl47","zr","bms","u7d","t2r","j0l"};
		DBApp.createTable("xxk", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("xxk.db"));
		String[] cols1 = {"a2y9","u95yj","qdg","ixu","ey","x39m"};
		DBApp.createTable("w2j", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("w2j.db"));
		String[] cols2 = {"wkcx"};
		DBApp.createTable("wtj", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("wtj.db"));
		String[] cols3 = {"zbz","t3"};
		DBApp.createTable("yp1", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("yp1.db"));
		String[] cols4 = {"f0q","ujw","an1","lv6tr","h9n","q82"};
		DBApp.createTable("u6t", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("u6t.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test76TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"sp","zboa1","s4a77","mgy"};
		DBApp.createTable("j9t", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("j9t.db"));
		String[] cols1 = {"nn55","nw4","inl0q","fnb","uj","ex"};
		DBApp.createTable("z6tv", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("z6tv.db"));
		String[] cols2 = {"gl","vp8","ym2","ct6","b4","mi7","z5"};
		DBApp.createTable("h0f8", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("h0f8.db"));
		String[] cols3 = {"n0","ir","zx","hu0","qg12r","s3f26","hk6"};
		DBApp.createTable("je183", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("je183.db"));
		String[] cols4 = {"dg7"};
		DBApp.createTable("a8i", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("a8i.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test77TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"dp0o2","s0t","txl3","cl185","zh10u"};
		DBApp.createTable("xg", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("xg.db"));
		String[] cols1 = {"hq83","x8c","da1u9","my150","l5q","qc","q5f28","x4806","m2ga","cdz"};
		DBApp.createTable("o6", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("o6.db"));
		String[] cols2 = {"xbv","q8495","q9i","n08","fy2","t9t6"};
		DBApp.createTable("v94l", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("v94l.db"));
		String[] cols3 = {"klo","u1","b0","ceyf","ut","d8a","cq","h0ab8","tbp4"};
		DBApp.createTable("osf", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("osf.db"));
		String[] cols4 = {"r6k"};
		DBApp.createTable("ew", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("ew.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test78TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a3","p58","e1u","m8v","hd7","cj"};
		DBApp.createTable("oemlt", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("oemlt.db"));
		String[] cols1 = {"xp8","cu53","z3","jy3r","lmpwk","rx2","k5sv","h3","ei338","g59"};
		DBApp.createTable("qb1", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("qb1.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test79TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"d3z6n","k1i4l","hwu","ypj08","wazxt","m94zz","z3p","j0i","jx0c","v6"};
		DBApp.createTable("nexso", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("nexso.db"));
		String[] cols1 = {"i221","z2h9","dw9ae","ugw2","ok735","z1oac","fk485","z5","gg"};
		DBApp.createTable("r37", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("r37.db"));
		String[] cols2 = {"n0","y9","r70","q46gs","k4n","g2wx","mwmq"};
		DBApp.createTable("y5", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("y5.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test80TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"arv","g02","g217","tf7x"};
		DBApp.createTable("b1b", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("b1b.db"));
		String[] cols1 = {"wdv6","c42f7"};
		DBApp.createTable("b16zv", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("b16zv.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test81TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"i9v7v","hhj","a22","zv"};
		DBApp.createTable("w8c31", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("w8c31.db"));
		String[] cols1 = {"sn","wf2","j1k","t75","ft","z1nx","rbxsp","v18","uwi"};
		DBApp.createTable("jaj", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("jaj.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test82TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"k8","sd","ar5s"};
		DBApp.createTable("by27v", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("by27v.db"));
		String[] cols1 = {"e9p1u","fby","ezqo","j3","d8cv9","g0xy"};
		DBApp.createTable("jy", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("jy.db"));
		String[] cols2 = {"y90p4","vt","n4","d3s","il","qdkt5"};
		DBApp.createTable("z78", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("z78.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test83TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"q8o","p5g","gq","x5y","g7","sm4"};
		DBApp.createTable("ot76c", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("ot76c.db"));
		String[] cols1 = {"z5ve","q8u","kmu","kn","a9zs"};
		DBApp.createTable("d5d8", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("d5d8.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test84TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"h4hz6","bo","cvxd6","h35","s1c","sje","d6p","qw9"};
		DBApp.createTable("ow7", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("ow7.db"));
		String[] cols1 = {"m6","e7","bicyr","yw","bsw5j","r5k","b0","b8x","w0"};
		DBApp.createTable("k77a", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("k77a.db"));
		String[] cols2 = {"vbm","zn1l4"};
		DBApp.createTable("d859", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("d859.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test85TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"djwb","sg1","x4597","i0p","vmx","rh6"};
		DBApp.createTable("fonx", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("fonx.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test86TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"x49","u63","n5t","jz","y44","fsf","y2d","q86"};
		DBApp.createTable("e9", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("e9.db"));
		String[] cols1 = {"hbj2","gp461","fl0u"};
		DBApp.createTable("dv", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("dv.db"));
		String[] cols2 = {"yrv0","b73o5","ij7ir","ti598","pa1","hl9"};
		DBApp.createTable("v6", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("v6.db"));
		String[] cols3 = {"v7u","m07","y5uf3","s3","tr7e","pt2b","lk","r8m","o68"};
		DBApp.createTable("h07nk", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("h07nk.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test87TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"jw0k9","y7q0a","ton","t38","s83","b83","cf","cm6","il","r478"};
		DBApp.createTable("sgd3", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("sgd3.db"));
		String[] cols1 = {"i4m42","o3","o3u","x39k","i6e","kq25","s2","h7k"};
		DBApp.createTable("s7n3", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("s7n3.db"));
		String[] cols2 = {"w4fq","k17","j1y9"};
		DBApp.createTable("n8r", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("n8r.db"));
		String[] cols3 = {"jr0","yz","z6","pv","ofnx","l19","yv35u","yw9ll","ptv86"};
		DBApp.createTable("gv6", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("gv6.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test88TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"wg9","c6e"};
		DBApp.createTable("e3rg", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("e3rg.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test89TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"v059"};
		DBApp.createTable("ynbc3", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("ynbc3.db"));
		String[] cols1 = {"qb","o3","w7","qp1f","ign","ims","uh","h7","xc05x"};
		DBApp.createTable("gyy8", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("gyy8.db"));
		String[] cols2 = {"ea54v","rn9t9","b2z3x","ll6","b45f","eyccl"};
		DBApp.createTable("b329", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("b329.db"));
		String[] cols3 = {"g6g","qy9","a0w","n2a60","u95i5","n7n","b2r","xf5k8","xz4jv","ztw2"};
		DBApp.createTable("omo", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("omo.db"));
		String[] cols4 = {"w5jq","m6f","w2y","b4r"};
		DBApp.createTable("ek441", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("ek441.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test90TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"q1r03","r6","r8","qs0","afr","q05m0","lio","rlu"};
		DBApp.createTable("r6b", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("r6b.db"));
		String[] cols1 = {"z9","ohn","g41","lm0"};
		DBApp.createTable("lx81", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("lx81.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test91TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"vk1","u9t0f","dt","h3795","e0k","z68","oj12n","i1f","dl49q","m8ca"};
		DBApp.createTable("iws00", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("iws00.db"));
		String[] cols1 = {"xo84"};
		DBApp.createTable("q48", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("q48.db"));
		String[] cols2 = {"v36","zmx","a34h","x881q","xcr8","ea"};
		DBApp.createTable("l46", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("l46.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test92TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"ahi","ftx","uh3","ume2","z2d19","h24","m3c0"};
		DBApp.createTable("e6h", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("e6h.db"));
		String[] cols1 = {"csa9","u806"};
		DBApp.createTable("c59", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("c59.db"));
		String[] cols2 = {"d1z81","m458"};
		DBApp.createTable("uw9", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("uw9.db"));
		String[] cols3 = {"u3t","d23","qob6","wb5","xyq","r914j","ov"};
		DBApp.createTable("o0w", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("o0w.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test93TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"o563","g59w0","s07","mlpr9","x5","o1u","axds","fj88","qb19p","kna4"};
		DBApp.createTable("zs5", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("zs5.db"));
		String[] cols1 = {"xyt","t4s","s8y","e6t","sp","f3v"};
		DBApp.createTable("j0y3", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("j0y3.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test94TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"c0sj","e05","bah","p48"};
		DBApp.createTable("am0", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("am0.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test95TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"fmy85","r44","nxuz","w3","wqci3"};
		DBApp.createTable("p0r", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("p0r.db"));
		String[] cols1 = {"wnc","azag","nbq","zo","m426","aan6","dqv","g69i3","q50"};
		DBApp.createTable("h3", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("h3.db"));
		String[] cols2 = {"j9","z9c","y271","hp02l","t93","k5"};
		DBApp.createTable("cxz74", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("cxz74.db"));
		String[] cols3 = {"e4","e49p4","n99","xq2","e73","a6z9","z7q64","or7u"};
		DBApp.createTable("lzw", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("lzw.db"));
		String[] cols4 = {"x4","ire6","xw51","f55","tn15","f6w","q08","pc8to"};
		DBApp.createTable("ge1q", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("ge1q.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test96TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"ml3","m06","m14","uh4","vb5p"};
		DBApp.createTable("p8lx", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("p8lx.db"));
		String[] cols1 = {"g8k80","zr4","u5b0r","k00","r5","w9j88","dj5","am7","r898k","gu4"};
		DBApp.createTable("a2o", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("a2o.db"));
		String[] cols2 = {"j90d8","dk","bc4","s472","wd2","x7v"};
		DBApp.createTable("k4h", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("k4h.db"));
		String[] cols3 = {"waa","t93","tds0j","ui","n29y0","po6","h0c80","nft4e","iq1","nxe"};
		DBApp.createTable("r20ko", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("r20ko.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test97TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"bbqi","r0031","kbk","y94","z83","sb","op","il","zkil3"};
		DBApp.createTable("nvb0x", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("nvb0x.db"));
		String[] cols1 = {"c31i5","x46","a0w","y355s","is6e","qyq","do75e"};
		DBApp.createTable("f3", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("f3.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test98TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"y8","yv4c6","vsso8","tz","y35jr"};
		DBApp.createTable("b541", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("b541.db"));
		String[] cols1 = {"r5a","lzt","vg10","yn7u","d4"};
		DBApp.createTable("d6u", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("d6u.db"));
		String[] cols2 = {"v8gwu","vn6","y9","n9z","f4d","av","tn1j"};
		DBApp.createTable("gu0q", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("gu0q.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test99TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"m5x","du1","b7t","w37","k3","ox512","y4","pap5e","sg26p"};
		DBApp.createTable("p89", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("p89.db"));
		String[] cols1 = {"nk7","ick","cu1","s80","kso","m3","itp"};
		DBApp.createTable("cj", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("cj.db"));
		String[] cols2 = {"c03","r9362","s3s","bdu","flo","upfj","o4obj"};
		DBApp.createTable("b981q", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("b981q.db"));
		String[] cols3 = {"b9","p8z6","ur2","c8ou0","yax8n"};
		DBApp.createTable("b2", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("b2.db"));
		String[] cols4 = {"b2s","u1rs","u15u","h9jn9","n5y","s487","n72","sny","j2","q6"};
		DBApp.createTable("wr", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("wr.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test100TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"jej","r1","iyq1","sc1","iib","nki","ur","g3d"};
		DBApp.createTable("hk18e", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("hk18e.db"));
		String[] cols1 = {"g6a","z00","l1a","xq","h9","g5e06","vp5"};
		DBApp.createTable("g87bh", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("g87bh.db"));
		String[] cols2 = {"vte3e","fps2u","f67v5","a01","emq"};
		DBApp.createTable("zr7iw", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("zr7iw.db"));
		String[] cols3 = {"i4ep","k2"};
		DBApp.createTable("rwj", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("rwj.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test101TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"x65"};
		DBApp.createTable("tz", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("tz.db"));
		String[] cols1 = {"n7","fp4s1"};
		DBApp.createTable("wfu", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("wfu.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test102TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"uq46","rv24u","mbws5","oxb4","jp8","e38v","mw78"};
		DBApp.createTable("v3", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("v3.db"));
		String[] cols1 = {"bd02p","o98","u34v","ashl","gp9b0","x3z"};
		DBApp.createTable("qn5", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("qn5.db"));
		String[] cols2 = {"jr3k"};
		DBApp.createTable("b949", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("b949.db"));
		String[] cols3 = {"hn","nh","zn7","t72","a84","og7l","w5890","sy5a"};
		DBApp.createTable("un6fh", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("un6fh.db"));
		String[] cols4 = {"p3","u67a","z9","tc8dc","k0gf","u3s","bjs88","tax","dzq"};
		DBApp.createTable("akw", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("akw.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test103TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"w6k","x2","w87","c9qmz","m0y","a49","jmz","my","o18"};
		DBApp.createTable("ivpf", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("ivpf.db"));
		String[] cols1 = {"pvnvo","p5q7","b158e","w61","s9x9","ad4","cu6","w8","qg"};
		DBApp.createTable("od6", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("od6.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test104TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"j08","osl27"};
		DBApp.createTable("i1o56", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("i1o56.db"));
		String[] cols1 = {"kpp","v2"};
		DBApp.createTable("rbcv8", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("rbcv8.db"));
		String[] cols2 = {"cmm4","u329o","k9","f93"};
		DBApp.createTable("t6", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("t6.db"));
		String[] cols3 = {"u0","wg","l5jn","c3s","huqt4","g0","fj4","x9"};
		DBApp.createTable("y56", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("y56.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test105TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"r3b","i51","w72eq","j41i","rl","gu233","ld4","q2a","g3d"};
		DBApp.createTable("tj0i1", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("tj0i1.db"));
		String[] cols1 = {"qh9s","d7m4","bx3h"};
		DBApp.createTable("nlc9", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("nlc9.db"));
		String[] cols2 = {"c5","zf"};
		DBApp.createTable("fzj", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("fzj.db"));
		String[] cols3 = {"b6340","ao0w","bw3","s0u","lyf","w6zh5"};
		DBApp.createTable("fvst", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("fvst.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test106TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"d3v","z6","vns","zkz60"};
		DBApp.createTable("a81v9", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("a81v9.db"));
		String[] cols1 = {"xo5","t40","q90p","qj5h4","tz","ia6la","q4u7","ct7vh","v8w"};
		DBApp.createTable("t5n0", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("t5n0.db"));
		String[] cols2 = {"t1g","q03"};
		DBApp.createTable("g63q", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("g63q.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test107TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"czh","lzj","osn","x59f8","sg58v","m55","n86","w4j","k7unk","nmn"};
		DBApp.createTable("j0jhw", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("j0jhw.db"));
		String[] cols1 = {"w23"};
		DBApp.createTable("kjz", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("kjz.db"));
		String[] cols2 = {"w2z","ws6o"};
		DBApp.createTable("ar", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("ar.db"));
		String[] cols3 = {"o82w","jmr","e28","b5a1"};
		DBApp.createTable("wj7", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("wj7.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test108TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"y1p66","tdt"};
		DBApp.createTable("j0kc1", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("j0kc1.db"));
		String[] cols1 = {"i7","x2d"};
		DBApp.createTable("l8", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("l8.db"));
		String[] cols2 = {"n7","dv","ubo"};
		DBApp.createTable("j6", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("j6.db"));
		String[] cols3 = {"zw4","y5","pexq0","b9k"};
		DBApp.createTable("al44", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("al44.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test109TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"jlx","s995w","rfk","a90","vh","xl5ld","s13","u8"};
		DBApp.createTable("j8", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("j8.db"));
		String[] cols1 = {"s03j","ok6","bl25z","nzn","tbx","w259a","xr2","m5","wl"};
		DBApp.createTable("w71y", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("w71y.db"));
		String[] cols2 = {"w34","y6dm4"};
		DBApp.createTable("v8", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("v8.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test110TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"w7p"};
		DBApp.createTable("lz7", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("lz7.db"));
		String[] cols1 = {"jg","bi5ok","mk","sya","by","jwkg","qa8","b2i"};
		DBApp.createTable("ao9s", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("ao9s.db"));
		String[] cols2 = {"ov9d","u5","m04"};
		DBApp.createTable("c6", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("c6.db"));
		String[] cols3 = {"m0d1h","xui","c92","nor","yeb03","p91l1"};
		DBApp.createTable("a6", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("a6.db"));
		String[] cols4 = {"i69ea","kl","u99q","d40","ap1","lh","d84","l2cq"};
		DBApp.createTable("auv5", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("auv5.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test111TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"i5t"};
		DBApp.createTable("fzu", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("fzu.db"));
		String[] cols1 = {"ba9","gag9","s2v","nr3","b3r9","k72js","ez9","ks","x19"};
		DBApp.createTable("v41", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("v41.db"));
		String[] cols2 = {"mo","jl7d7","sg5","wn55","y4s","mj8a","j398","j7jz9"};
		DBApp.createTable("ct", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("ct.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test112TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"jvz2","odeq","bt"};
		DBApp.createTable("g3k", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("g3k.db"));
		String[] cols1 = {"qzu5"};
		DBApp.createTable("bv", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("bv.db"));
		String[] cols2 = {"x19v","bjn","sk7"};
		DBApp.createTable("yh4", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("yh4.db"));
		String[] cols3 = {"dt95p","hmy45","pvu"};
		DBApp.createTable("j78", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("j78.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test113TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"cy5","g4mq","ly5e0","g7q","i26","s8wzs","q5","n75"};
		DBApp.createTable("p56fr", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("p56fr.db"));
		String[] cols1 = {"h0","of","e7","a0","crk","ygr9","q5by","s26","a3"};
		DBApp.createTable("y5o", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("y5o.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test114TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"k29y","r98q","mjun","y47","xb"};
		DBApp.createTable("smk", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("smk.db"));
		String[] cols1 = {"z14","y88","ebz","cq","jp3","b96"};
		DBApp.createTable("k8193", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("k8193.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test115TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"a134"};
		DBApp.createTable("u48a6", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("u48a6.db"));
		String[] cols1 = {"c7","s89","z57","wwe3","ptm","ce0","xfi","h14","sg2m","etl"};
		DBApp.createTable("t21lu", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("t21lu.db"));
		String[] cols2 = {"l5g","ofd","j3aod","k6","kq0u0","s7","j631v","kfq8z","cm"};
		DBApp.createTable("kn", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("kn.db"));
		String[] cols3 = {"z02v"};
		DBApp.createTable("t2no8", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("t2no8.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test116TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"u5","m1"};
		DBApp.createTable("g80", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("g80.db"));
		String[] cols1 = {"l4a","x51","qk7n","c24m8","khc","er5","ni01","le","u2z"};
		DBApp.createTable("k4r", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("k4r.db"));
		String[] cols2 = {"zih8","j0s4","p55","xo94","j2123","a2","m5d","eikc","mg3a0"};
		DBApp.createTable("zbi1d", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("zbi1d.db"));
		String[] cols3 = {"uv","tyzg","p06","ktjz"};
		DBApp.createTable("vcj", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("vcj.db"));
		String[] cols4 = {"ifs4z","zd","vc0a6","lra","kc5t"};
		DBApp.createTable("ks", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("ks.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test117TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"sk4","aq8","vg97","q8d","kx8","inl3","c6o","s0b","krv"};
		DBApp.createTable("zjo44", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("zjo44.db"));
		String[] cols1 = {"y6z","p6"};
		DBApp.createTable("s6", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("s6.db"));
		String[] cols2 = {"u4f04","cnb","op1","c1y09"};
		DBApp.createTable("kx6", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("kx6.db"));
		String[] cols3 = {"k30","es0o9","j86","d85","h58","g3","qqv"};
		DBApp.createTable("d2k08", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("d2k08.db"));
		String[] cols4 = {"ay","it","ced6h","gy"};
		DBApp.createTable("yei7", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("yei7.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test118TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"xstw","wmd","j4gs9"};
		DBApp.createTable("ki2ko", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("ki2ko.db"));
		String[] cols1 = {"z0u","u9l"};
		DBApp.createTable("w9", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("w9.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test119TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"r8","q2","b7h53","kxk","by8","c9","awi6","oi6"};
		DBApp.createTable("nl2zu", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("nl2zu.db"));
		String[] cols1 = {"x5ih","h9m9","kb","gp6r","s8r5l","g9qim","se"};
		DBApp.createTable("s25", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("s25.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test120TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"d92","ha9","bafq","t759","tkx","y89u","kvb"};
		DBApp.createTable("xe8", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("xe8.db"));
		String[] cols1 = {"i9o","t6","mqi","lup","nf1p8","iun","m721","p0m"};
		DBApp.createTable("vd7", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("vd7.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test121TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"njc","o14","dj","ko6","aque8"};
		DBApp.createTable("o7", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("o7.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test122TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"x2","i0j4","a7w5","g911h","z5f09","y35iv","qjbd","rz0o"};
		DBApp.createTable("ep", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("ep.db"));
		String[] cols1 = {"n3t","ys9","ap0","w34","z5"};
		DBApp.createTable("yw", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("yw.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test123TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"y1a","r8"};
		DBApp.createTable("t01", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("t01.db"));
		String[] cols1 = {"bad","ox38","ldnm0","n9k10","v0j","t1d"};
		DBApp.createTable("ww", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("ww.db"));
		String[] cols2 = {"w3","p12","x427f","zkf","ayc","ff","gb","cl","s07","j87a4"};
		DBApp.createTable("wd35", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("wd35.db"));
		String[] cols3 = {"qhn","ui8lh","fa","i6er","f7u6a","xb7k5"};
		DBApp.createTable("nd3", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("nd3.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test124TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"rso","m1","b0w15","wjmb","i04","zjs","f3"};
		DBApp.createTable("vq", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("vq.db"));
		String[] cols1 = {"ef3","k2g"};
		DBApp.createTable("cm7s7", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("cm7s7.db"));
		String[] cols2 = {"xoi39","fg","zn","k4","kz74i","dmxc","tce0h","kq8","pf489","yx9"};
		DBApp.createTable("d8t", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("d8t.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test125TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"qa9a","gl1c"};
		DBApp.createTable("ifk", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("ifk.db"));
		String[] cols1 = {"r7k8","vp5","e10n","qb9"};
		DBApp.createTable("z5t7", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("z5t7.db"));
		String[] cols2 = {"n0q21","hu6","jqq5","gcu","r2v","i7","s55"};
		DBApp.createTable("gd", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("gd.db"));
		String[] cols3 = {"y0v","on955","m64","ss7a","e9d","sc6"};
		DBApp.createTable("vtm", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("vtm.db"));
		String[] cols4 = {"od2","zj11e","hs8uh"};
		DBApp.createTable("y62", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("y62.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test126TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"xw9","cic","by9","n51"};
		DBApp.createTable("z61yv", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("z61yv.db"));
		String[] cols1 = {"ui","qc","mbb","vd","dfn","vpfz8"};
		DBApp.createTable("qm5", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("qm5.db"));
		String[] cols2 = {"d9","hkk","se33","auts","u2p"};
		DBApp.createTable("vy4dt", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("vy4dt.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test127TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"qhx","bd","g58f","umz","mz","s84","p8x4","z4y","syrv"};
		DBApp.createTable("n16", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("n16.db"));
		String[] cols1 = {"s60","z70","dv2","dz","fh7","r68"};
		DBApp.createTable("ffz4", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("ffz4.db"));
		String[] cols2 = {"w9gw6"};
		DBApp.createTable("ka4", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("ka4.db"));
		String[] cols3 = {"oz8s","fs0","z8","kpe","z0x","dkm5a","n5i","d4o","lsrl"};
		DBApp.createTable("mfx", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("mfx.db"));
		String[] cols4 = {"je4ep","g3","de4","dapb8","ydq","f6","zte67","ffdd","yya","p4x99"};
		DBApp.createTable("v0", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("v0.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test128TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"kh","yt82","wr0mn","k8","d0"};
		DBApp.createTable("hc8n", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("hc8n.db"));
		String[] cols1 = {"dmj","n7p","w12","k0","q9q"};
		DBApp.createTable("s88", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("s88.db"));
		String[] cols2 = {"iy5","t50","a5g","w1f","o2s","pus","o613","u66","q4x","rv809"};
		DBApp.createTable("a2", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("a2.db"));
		String[] cols3 = {"kdq4"};
		DBApp.createTable("e1", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("e1.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test129TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"ht3z3","oq60n","p91w7","s00","uvdll","g13tf","mug"};
		DBApp.createTable("px1", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("px1.db"));
		String[] cols1 = {"i6w0","on","snzn","rjp","wvw"};
		DBApp.createTable("pemo", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("pemo.db"));
		String[] cols2 = {"uzb1"};
		DBApp.createTable("ygun", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("ygun.db"));
		String[] cols3 = {"i0","g51","p0vk4"};
		DBApp.createTable("dq", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("dq.db"));
		String[] cols4 = {"ejj5v","ihu6e","g7","ms07","qa","ix8","wwp","c2yo"};
		DBApp.createTable("ob5g", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("ob5g.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test130TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"y2us","a4","j1","n2k6","zt41","z19","ffk4","p1m"};
		DBApp.createTable("ujb", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("ujb.db"));
		String[] cols1 = {"wyo","ef2","dof","aus","i1","j73","zih"};
		DBApp.createTable("v605a", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("v605a.db"));
		String[] cols2 = {"h50","e6q","cn","qg"};
		DBApp.createTable("nvp", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("nvp.db"));
		String[] cols3 = {"d2q43","kx5qy","addx8","fuc","da","gl","tjmyr","px5","n59ic"};
		DBApp.createTable("rq5", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("rq5.db"));
		String[] cols4 = {"ja0c","nj5nn","u3","z3","p47q","d20b","qi2","i3u"};
		DBApp.createTable("d62o", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("d62o.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test131TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"emj","yq1","xu7tp","vr8","v57","dg3","ng1","b13c"};
		DBApp.createTable("jm56", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("jm56.db"));
		String[] cols1 = {"ak2","el","vly29","h4u","f43","s146","of","y9r","u8zk5"};
		DBApp.createTable("x2t", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("x2t.db"));
		String[] cols2 = {"cx0","o4","t9d","s7s61","j3"};
		DBApp.createTable("i5n", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("i5n.db"));
		String[] cols3 = {"htd5","j00","q0tz","qmtu","f8","q82","dxd","w87u"};
		DBApp.createTable("pmc", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("pmc.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test132TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"i3","x7k","ab7g"};
		DBApp.createTable("bz7", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("bz7.db"));
		String[] cols1 = {"fmt","si5"};
		DBApp.createTable("tk", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("tk.db"));
		String[] cols2 = {"u52"};
		DBApp.createTable("e11", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("e11.db"));
		String[] cols3 = {"qlq59","v7s","vo8k","fk171"};
		DBApp.createTable("oao7", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("oao7.db"));
		String[] cols4 = {"d8v0","k8s3","moxc"};
		DBApp.createTable("i544", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("i544.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test133TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"s46o","ob9a","qk5","t8","fu3d9","jp660","d290","u2r"};
		DBApp.createTable("i0g", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("i0g.db"));
		String[] cols1 = {"t2v","yxm"};
		DBApp.createTable("ot3", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("ot3.db"));
		String[] cols2 = {"ww"};
		DBApp.createTable("v0dw", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("v0dw.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test134TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"k1n","b0","j9","xe65"};
		DBApp.createTable("p4y", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("p4y.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test135TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"s2","pd0","h8j","tg7b"};
		DBApp.createTable("d30", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("d30.db"));
		String[] cols1 = {"p566t","k198","x618","e0"};
		DBApp.createTable("d366o", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("d366o.db"));
		String[] cols2 = {"faq"};
		DBApp.createTable("cu", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("cu.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test136TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"zzymh","i7","sjt"};
		DBApp.createTable("tp", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("tp.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test137TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"w20o","v73","b9mr","en8w","j0","z0","d33","t3","s29f"};
		DBApp.createTable("f73", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("f73.db"));
		String[] cols1 = {"vw4","r683n","j44","psqt","c5k","eu0","pu"};
		DBApp.createTable("a1f", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("a1f.db"));
		String[] cols2 = {"z0l","lz","fm9","ye6","v3t","hm0","g69","t4t15","y45"};
		DBApp.createTable("ok8q", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("ok8q.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test138TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"i9","jt3","nf9","a94l","r2dh","jp","d4","n02","j8"};
		DBApp.createTable("srm", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("srm.db"));
		String[] cols1 = {"ok3"};
		DBApp.createTable("ecv8o", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("ecv8o.db"));
		String[] cols2 = {"aw7","ij","y4t","xe0"};
		DBApp.createTable("o2", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("o2.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test139TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"xl7","r1v4","gm8gy","p2","w5","z425t","f8qav"};
		DBApp.createTable("pg", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("pg.db"));
		String[] cols1 = {"gx9","f8e9","u8","k46in"};
		DBApp.createTable("trg", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("trg.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test140TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"be5"};
		DBApp.createTable("tb9", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("tb9.db"));
		String[] cols1 = {"lc3","bra9","jib","q7p","f2d","l135p"};
		DBApp.createTable("ch", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("ch.db"));
		String[] cols2 = {"n76","a5","cd0","qgn","fw","h55f","g3vd","xr7yb","yc90","j9"};
		DBApp.createTable("i4", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("i4.db"));
		String[] cols3 = {"pkz","nvma","z3","pa9c3","pfs"};
		DBApp.createTable("pj5", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("pj5.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test141TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"n73","wdm","ee6","bn9tg","vu8","ks","f30","wf","vw89p","t6"};
		DBApp.createTable("gq8", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("gq8.db"));
		String[] cols1 = {"r035"};
		DBApp.createTable("w2aji", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("w2aji.db"));
		String[] cols2 = {"q37","wxl","rle","y54t","ujy"};
		DBApp.createTable("oz", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("oz.db"));
		String[] cols3 = {"er48","bg0q8","o6","tqllk","svi","mu","tq07","it3","xdi"};
		DBApp.createTable("el3c", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("el3c.db"));
		String[] cols4 = {"b4n7","l35","n0d","nch","j2","gr2"};
		DBApp.createTable("buq", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("buq.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test142TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"c26s6","ayv","iv3"};
		DBApp.createTable("x6w9", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("x6w9.db"));
		String[] cols1 = {"qo","rhh","m6j"};
		DBApp.createTable("jr", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("jr.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test143TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"y6k","bcp11","nfrk","vw","pg7","b0819","eey","ip","t27","rc21"};
		DBApp.createTable("ogk", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("ogk.db"));
		String[] cols1 = {"j5v","pqz"};
		DBApp.createTable("gj9h5", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("gj9h5.db"));
		String[] cols2 = {"ad3t3","xfj","f6","k7","y7","cns"};
		DBApp.createTable("td0", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("td0.db"));
		String[] cols3 = {"sqaqx","dk","d23","o30","e9"};
		DBApp.createTable("i83", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("i83.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test144TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"y272","e26","pj","pv","bs","g5z","y16"};
		DBApp.createTable("n2q02", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("n2q02.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test145TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"n7","t1im2","j5l","m9o","k49"};
		DBApp.createTable("isx", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("isx.db"));
		String[] cols1 = {"x3","bqu","t130","a9","t5","csj","oi02","iucr","b4j3","u1633"};
		DBApp.createTable("zjs8", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("zjs8.db"));
		String[] cols2 = {"d327","a336","j04n","dzv","vy65","bi7k"};
		DBApp.createTable("iv3t", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("iv3t.db"));
		String[] cols3 = {"eo7","kti1","r5"};
		DBApp.createTable("s46", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("s46.db"));
		String[] cols4 = {"x75","w25","fp","l93jb","k94","ji199","b417l","fgw","w7o"};
		DBApp.createTable("g1c", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("g1c.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test146TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"l3t1t","a3","fw1","szz1","x5kc6","jj1","x76","rk316","v8o1","if"};
		DBApp.createTable("u5p96", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("u5p96.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test147TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"g01t","a8x6h","f39","xlpg","p5w","vkhli"};
		DBApp.createTable("kykf", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("kykf.db"));
		String[] cols1 = {"e61l9","ndki"};
		DBApp.createTable("t0fw", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("t0fw.db"));
		String[] cols2 = {"r2t","sah","ipg","oj4","g3m65","z2n"};
		DBApp.createTable("whi1", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("whi1.db"));
		String[] cols3 = {"w19o8","s8wj","q0hn","ml67e","x45","q5d9","g04","t851g","c9v2","g2g"};
		DBApp.createTable("lc", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("lc.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test148TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"ski6","g46","g3c"};
		DBApp.createTable("ra093", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("ra093.db"));
		String[] cols1 = {"c767","xhy1","e93","yp","c6","m5p","h8cv","y4i","xr1"};
		DBApp.createTable("ne", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("ne.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test149TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"s6","fo2","g84","y3up1"};
		DBApp.createTable("wft7", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("wft7.db"));
		String[] cols1 = {"vn","d5c2i","r7728","a2","rqj","y9","ujz"};
		DBApp.createTable("lw6", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("lw6.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test150TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"r0vme","ify","g5","v1","bnvgx","b04sn","o099","d66d5","ctx","t089h"};
		DBApp.createTable("e877", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("e877.db"));
		String[] cols1 = {"mf"};
		DBApp.createTable("z80", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("z80.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test151TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"eo633","c4p5","jn1e"};
		DBApp.createTable("nz", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("nz.db"));
		String[] cols1 = {"h9o"};
		DBApp.createTable("mj07t", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("mj07t.db"));
		String[] cols2 = {"lbw3","jq1","x983a"};
		DBApp.createTable("gk3", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("gk3.db"));
		String[] cols3 = {"ll7"};
		DBApp.createTable("ak1r", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("ak1r.db"));
		String[] cols4 = {"ho30","cj14"};
		DBApp.createTable("qt", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("qt.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test152TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"jhj","t0","l7","kv6n","rt4","dul0","hp09","c4"};
		DBApp.createTable("c2i", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("c2i.db"));
		String[] cols1 = {"pt5","hj553","j7p"};
		DBApp.createTable("tdqw7", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("tdqw7.db"));
		String[] cols2 = {"it828","fn"};
		DBApp.createTable("d0", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("d0.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test153TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"f66e9"};
		DBApp.createTable("c17", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("c17.db"));
		String[] cols1 = {"nx54n"};
		DBApp.createTable("rdt", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("rdt.db"));
		String[] cols2 = {"p83b","n1y","lvr","v1v","l8q","p23","ea8","nr0","p2","kh3"};
		DBApp.createTable("u1", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("u1.db"));
		String[] cols3 = {"ed2","n8t","t05","ff3i","s04","v9l","u344i","j2l9"};
		DBApp.createTable("fzun", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("fzun.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test154TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"u9"};
		DBApp.createTable("q14", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("q14.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test155TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"c616","br3b","riq"};
		DBApp.createTable("wcn", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("wcn.db"));
		String[] cols1 = {"ymko","u60zq","b8z9","iaw","rvs06","uxf","t92v","ha","e62d"};
		DBApp.createTable("o6ey3", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("o6ey3.db"));
		String[] cols2 = {"oc"};
		DBApp.createTable("lu2h", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("lu2h.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test156TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"o23m8","c15","q2928","n29l","kv","n3v","n65","ict","vki"};
		DBApp.createTable("kh72", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("kh72.db"));
		String[] cols1 = {"n4"};
		DBApp.createTable("mhs2r", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("mhs2r.db"));
		String[] cols2 = {"avpp6","sp0","ph38r","z4","md"};
		DBApp.createTable("ahv3", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("ahv3.db"));
		String[] cols3 = {"n52","u6y","gex","y3x","t02","dely"};
		DBApp.createTable("f94", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("f94.db"));
		String[] cols4 = {"o0h","i180s","zhr6b","fukt9","d775","t2678","j522i","p6l0","o5"};
		DBApp.createTable("jnvjh", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("jnvjh.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test157TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"zjoo","y3mp6","a92","dae","dkilw","va08o","oio"};
		DBApp.createTable("ny", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("ny.db"));
		String[] cols1 = {"ixfa","s6z0u","p0","lwl","m740"};
		DBApp.createTable("w85", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("w85.db"));
		String[] cols2 = {"tj8","u37","bj4nu","oy","mxo7k"};
		DBApp.createTable("cnt", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("cnt.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test158TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"bdz"};
		DBApp.createTable("x6y", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("x6y.db"));
		String[] cols1 = {"n9748","jd5","lk584","b43b7","t6","qo506","ic","s9sl","k5e01","n7"};
		DBApp.createTable("h64", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("h64.db"));
		String[] cols2 = {"f9k","r4g","b081","r5t"};
		DBApp.createTable("a06", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("a06.db"));
		String[] cols3 = {"s9r","lskjx","jv","xb","wy6"};
		DBApp.createTable("g2r01", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("g2r01.db"));
		String[] cols4 = {"y924"};
		DBApp.createTable("g6", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("g6.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test159TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"x3udn","ka6","f2","lgh","e89","f6vc","b16"};
		DBApp.createTable("vt5", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("vt5.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test160TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"a90"};
		DBApp.createTable("js60n", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("js60n.db"));
		String[] cols1 = {"x9iz","yh","ukg","v2","m65y","j53","p567","o787g","bk221","vbl3"};
		DBApp.createTable("fyu", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("fyu.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test161TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"r2708","c19","y05a","naxl","wqv","bq9d","h6"};
		DBApp.createTable("j9f", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("j9f.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test162TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"wzhg8"};
		DBApp.createTable("dwn", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("dwn.db"));
		String[] cols1 = {"bw2wj","p10","uvk","nm"};
		DBApp.createTable("v7", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("v7.db"));
		String[] cols2 = {"p2","urv","m5k","z5","lzh"};
		DBApp.createTable("y6", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("y6.db"));
		String[] cols3 = {"il8","yoev","y7","t10","s68","rh4"};
		DBApp.createTable("wm", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("wm.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test163TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"e1b","t4z","gc","a0sq","e8ui1","u38u0","qr6p"};
		DBApp.createTable("gl9", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("gl9.db"));
		String[] cols1 = {"g3","v5s38","uqon","tzw9","sa9","xib7"};
		DBApp.createTable("d14", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("d14.db"));
		String[] cols2 = {"f6","m0","jf410","f19","d133a","ss9a","s5","twkm"};
		DBApp.createTable("vo0", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("vo0.db"));
		String[] cols3 = {"t6","paa","ks91","tr","u7","xb5","p8"};
		DBApp.createTable("vf", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("vf.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test164TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"n33"};
		DBApp.createTable("l8", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("l8.db"));
		String[] cols1 = {"w722","ebg","h0"};
		DBApp.createTable("t436", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("t436.db"));
		String[] cols2 = {"fj7d","ly","o92","q4gn","k4u","bp4l","uz4m","m73","e0","aex"};
		DBApp.createTable("s317", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("s317.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test165TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"qt2","x624","y26","qug","b5","xf5i6"};
		DBApp.createTable("ce2e0", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("ce2e0.db"));
		String[] cols1 = {"n58o","xk2"};
		DBApp.createTable("n45", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("n45.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test166TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"pf0x3","m99c","g621l","yi3","d2u","q8z"};
		DBApp.createTable("v5", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("v5.db"));
		String[] cols1 = {"jt","y3j8i"};
		DBApp.createTable("a8", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("a8.db"));
		String[] cols2 = {"o50","cn"};
		DBApp.createTable("ce3c8", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("ce3c8.db"));
		String[] cols3 = {"vhfx"};
		DBApp.createTable("ogn", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("ogn.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test167TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"l55y4"};
		DBApp.createTable("rr", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("rr.db"));
		String[] cols1 = {"jfd","vxmd9","xu4"};
		DBApp.createTable("m0", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("m0.db"));
		String[] cols2 = {"v5"};
		DBApp.createTable("l767", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("l767.db"));
		String[] cols3 = {"j4az","z6","nf0","nr50e","u33r","mp65","v4","z27","l5q4"};
		DBApp.createTable("m206", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("m206.db"));
		String[] cols4 = {"q14","t9it","ct8q","ceov2","u6wr","b42","hp8","f5c8"};
		DBApp.createTable("mu7", cols4);
		String fileManagerTrace4 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace4.contains("mu7.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test168TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"j54"};
		DBApp.createTable("zhc", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("zhc.db"));
		String[] cols1 = {"iw","f8oo","uho","a04j"};
		DBApp.createTable("vrg", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("vrg.db"));
		String[] cols2 = {"v6mu","ic"};
		DBApp.createTable("t606o", cols2);
		String fileManagerTrace2 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace2.contains("t606o.db"));
		String[] cols3 = {"m2","h8x","op2","er8","nm","ka0d1","b3ed","nx7","gzxt"};
		DBApp.createTable("m2t", cols3);
		String fileManagerTrace3 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace3.contains("m2t.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test169TableSerializationDBApp() throws Exception
	{
		DBApp.dataPageSize = 100;
		FileManager.reset();
	
		String[] cols0 = {"zx0ah","a8","iyf"};
		DBApp.createTable("g84", cols0);
		String fileManagerTrace0 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace0.contains("g84.db"));
		String[] cols1 = {"rc59","k2","d37","y6ukj","qya","s0e4k","i6e","w3"};
		DBApp.createTable("uj", cols1);
		String fileManagerTrace1 = FileManager.trace();
		assertTrue("A .db file should be created for each table.", fileManagerTrace1.contains("uj.db"));
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test0TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"f44"};
		DBApp.createTable("vyk", cols0);
		String tableTrace0 = DBApp.getLastTrace("vyk");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("vyk"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"q0ch9","c97"};
		DBApp.createTable("hh6", cols1);
		String tableTrace1 = DBApp.getLastTrace("hh6");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("hh6"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"kqo9"};
		DBApp.createTable("fj4oz", cols2);
		String tableTrace2 = DBApp.getLastTrace("fj4oz");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("fj4oz"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test1TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"n7","xs7","q77","sw12","u08s9","h2a3"};
		DBApp.createTable("t2c", cols0);
		String tableTrace0 = DBApp.getLastTrace("t2c");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("t2c"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"l7sb"};
		DBApp.createTable("uvo", cols1);
		String tableTrace1 = DBApp.getLastTrace("uvo");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("uvo"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test2TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"to","wun","z7067","vyl0","b498","k26","em6y8","g9hjj","yz59","x341t"};
		DBApp.createTable("d6yw", cols0);
		String tableTrace0 = DBApp.getLastTrace("d6yw");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("d6yw"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"juq3x","db","cpgm5","h5","mp7","gv5b","d53","b936"};
		DBApp.createTable("bg2l", cols1);
		String tableTrace1 = DBApp.getLastTrace("bg2l");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("bg2l"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"e00","tzh","h4qla","t7z7z","d6","qyq","d65","q4e","z95o","ao7"};
		DBApp.createTable("gt5", cols2);
		String tableTrace2 = DBApp.getLastTrace("gt5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("gt5"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"kxbt","wc","b2"};
		DBApp.createTable("o5", cols3);
		String tableTrace3 = DBApp.getLastTrace("o5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("o5"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"p48","a68","hw","a2","is136","f4494","m5j","z46","g2","b9dl3"};
		DBApp.createTable("lte2", cols4);
		String tableTrace4 = DBApp.getLastTrace("lte2");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("lte2"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test3TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"bfsf","ca","a9bn0"};
		DBApp.createTable("ht", cols0);
		String tableTrace0 = DBApp.getLastTrace("ht");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("ht"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"kh3","y9","w6c4","c20c","z0j","w33"};
		DBApp.createTable("vr7", cols1);
		String tableTrace1 = DBApp.getLastTrace("vr7");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("vr7"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"m1q","kq3q9"};
		DBApp.createTable("pua", cols2);
		String tableTrace2 = DBApp.getLastTrace("pua");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("pua"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"y0sg"};
		DBApp.createTable("qa6h", cols3);
		String tableTrace3 = DBApp.getLastTrace("qa6h");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("qa6h"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test4TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"gh8m","q4","q417","oe","u71zn","l8y0j"};
		DBApp.createTable("p9qh0", cols0);
		String tableTrace0 = DBApp.getLastTrace("p9qh0");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("p9qh0"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"t29","g59","w822","t6q"};
		DBApp.createTable("t11", cols1);
		String tableTrace1 = DBApp.getLastTrace("t11");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("t11"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"t6op"};
		DBApp.createTable("ivtws", cols2);
		String tableTrace2 = DBApp.getLastTrace("ivtws");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("ivtws"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"j7","h907","t5g"};
		DBApp.createTable("n4k", cols3);
		String tableTrace3 = DBApp.getLastTrace("n4k");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("n4k"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test5TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"nr","fr5e","k40","r8j","t6ij1","ktl04"};
		DBApp.createTable("d26h", cols0);
		String tableTrace0 = DBApp.getLastTrace("d26h");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("d26h"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"bvo81","go770","a0h","i23","d1k","v2w5f","q9f1","lbx66","er10m","x2y"};
		DBApp.createTable("fv", cols1);
		String tableTrace1 = DBApp.getLastTrace("fv");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("fv"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"a45","cl3","nt1f","gra","p278","sinci","wn5","f30","q8wdv","a38"};
		DBApp.createTable("snhwl", cols2);
		String tableTrace2 = DBApp.getLastTrace("snhwl");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("snhwl"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"dnia","s0z21","f92","f7nx6","l3"};
		DBApp.createTable("dt", cols3);
		String tableTrace3 = DBApp.getLastTrace("dt");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("dt"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"s98","w0","x4c","a4h","p4c9","r77w","jn0","dfp0","mg9d"};
		DBApp.createTable("e0a", cols4);
		String tableTrace4 = DBApp.getLastTrace("e0a");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("e0a"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test6TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"e72k","k0f","xuv","ef7","xj5l","m3y","tit0l","wl","h87"};
		DBApp.createTable("x2o", cols0);
		String tableTrace0 = DBApp.getLastTrace("x2o");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("x2o"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"s4a","g15","g27","a71"};
		DBApp.createTable("g15b0", cols1);
		String tableTrace1 = DBApp.getLastTrace("g15b0");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("g15b0"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"mg7","k82","oie96","pq","j0r2r","s9","x06k"};
		DBApp.createTable("s6l", cols2);
		String tableTrace2 = DBApp.getLastTrace("s6l");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("s6l"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test7TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"xj157","hyt","loex0","jt","ll14l","oj8"};
		DBApp.createTable("l41i", cols0);
		String tableTrace0 = DBApp.getLastTrace("l41i");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("l41i"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"y38","ft9","a3s","wifo","i8s55","rfqig","k6","pv5"};
		DBApp.createTable("ak", cols1);
		String tableTrace1 = DBApp.getLastTrace("ak");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("ak"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"lt0","nv2j","gr","yq"};
		DBApp.createTable("t18", cols2);
		String tableTrace2 = DBApp.getLastTrace("t18");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("t18"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test8TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"yj1","me","ap","nx","d8u2","jjq","mcqk4","f75","oi6r5"};
		DBApp.createTable("k50y", cols0);
		String tableTrace0 = DBApp.getLastTrace("k50y");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("k50y"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"h0","o2u8k","pf0","ac","mxo","k6"};
		DBApp.createTable("mk3be", cols1);
		String tableTrace1 = DBApp.getLastTrace("mk3be");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("mk3be"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"oe6","aq2","vy0","r79","fn","uv","w88"};
		DBApp.createTable("a39x1", cols2);
		String tableTrace2 = DBApp.getLastTrace("a39x1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("a39x1"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"z4","k3","ipc05","td","p6w","s64j","uu86","bvo"};
		DBApp.createTable("ly9g", cols3);
		String tableTrace3 = DBApp.getLastTrace("ly9g");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("ly9g"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"rz3f","hym","g8ar7"};
		DBApp.createTable("b5k", cols4);
		String tableTrace4 = DBApp.getLastTrace("b5k");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("b5k"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test9TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"zan"};
		DBApp.createTable("iq", cols0);
		String tableTrace0 = DBApp.getLastTrace("iq");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("iq"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"g4","yxgcq"};
		DBApp.createTable("fs3", cols1);
		String tableTrace1 = DBApp.getLastTrace("fs3");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("fs3"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"wcl","wenw","fj4e4"};
		DBApp.createTable("b9c", cols2);
		String tableTrace2 = DBApp.getLastTrace("b9c");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("b9c"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"en","um9","u01q","le4","u2"};
		DBApp.createTable("ddl", cols3);
		String tableTrace3 = DBApp.getLastTrace("ddl");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("ddl"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test10TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"zcp23","n3","w7","a33","e975z","a4pl"};
		DBApp.createTable("h0sp6", cols0);
		String tableTrace0 = DBApp.getLastTrace("h0sp6");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("h0sp6"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test11TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"d6","ehc5","c0","snr","s9v","di98x"};
		DBApp.createTable("voz", cols0);
		String tableTrace0 = DBApp.getLastTrace("voz");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("voz"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"zdb","uz69o","fd"};
		DBApp.createTable("ir0", cols1);
		String tableTrace1 = DBApp.getLastTrace("ir0");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("ir0"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"y6g70"};
		DBApp.createTable("gixo", cols2);
		String tableTrace2 = DBApp.getLastTrace("gixo");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("gixo"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"mi","lu4xi","iu1yc","hxd","hxfo","e0d4c","t7soi","r8rg","g7e9"};
		DBApp.createTable("oj3", cols3);
		String tableTrace3 = DBApp.getLastTrace("oj3");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("oj3"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"o4","d2z"};
		DBApp.createTable("kk004", cols4);
		String tableTrace4 = DBApp.getLastTrace("kk004");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("kk004"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test12TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"p75f"};
		DBApp.createTable("zk", cols0);
		String tableTrace0 = DBApp.getLastTrace("zk");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("zk"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"vqz5p","s7l","v50d","byr","wdh","iqe","p18"};
		DBApp.createTable("zuewb", cols1);
		String tableTrace1 = DBApp.getLastTrace("zuewb");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("zuewb"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test13TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"fo","fw8s","s70","mxhp","m4kju","jkx","jq06","n1l6","b8k"};
		DBApp.createTable("yfld", cols0);
		String tableTrace0 = DBApp.getLastTrace("yfld");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("yfld"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test14TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"s9w2","ok3","bo5","o769","u6","ka"};
		DBApp.createTable("fxc", cols0);
		String tableTrace0 = DBApp.getLastTrace("fxc");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("fxc"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"v9e","h2105","j86qc","jo4","qlu","w4o9","mhc","x36","og2u","dne"};
		DBApp.createTable("yivu", cols1);
		String tableTrace1 = DBApp.getLastTrace("yivu");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("yivu"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"caqs","jnizu","v92","z2","b1","n9","iyt","bw"};
		DBApp.createTable("v1s", cols2);
		String tableTrace2 = DBApp.getLastTrace("v1s");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("v1s"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"c9l","t3ju","ek2","x6p","k7cm","n43","ovh","i9q","uy9o1","zf37"};
		DBApp.createTable("koe97", cols3);
		String tableTrace3 = DBApp.getLastTrace("koe97");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("koe97"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"e735","d5"};
		DBApp.createTable("qz2", cols4);
		String tableTrace4 = DBApp.getLastTrace("qz2");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("qz2"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test15TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"hj7"};
		DBApp.createTable("x2n", cols0);
		String tableTrace0 = DBApp.getLastTrace("x2n");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("x2n"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"q8v3","y0","b53","j9e2","x71s","ff","e3s5","ai8","q09","j3s"};
		DBApp.createTable("bz", cols1);
		String tableTrace1 = DBApp.getLastTrace("bz");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("bz"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"p11a9","ev1","i1","ywy2","e7","x8"};
		DBApp.createTable("u1", cols2);
		String tableTrace2 = DBApp.getLastTrace("u1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("u1"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test16TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"yun0h","ujc","xt"};
		DBApp.createTable("w6r", cols0);
		String tableTrace0 = DBApp.getLastTrace("w6r");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("w6r"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"o44","wed","nf5","hj","e3y"};
		DBApp.createTable("mc", cols1);
		String tableTrace1 = DBApp.getLastTrace("mc");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("mc"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"r43","p4f","vwdv","y0ln5","ot","hzu","s9e96","p1k"};
		DBApp.createTable("w3gk8", cols2);
		String tableTrace2 = DBApp.getLastTrace("w3gk8");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("w3gk8"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"x8","qa5k8","o1ejj","g50","ej9","ofjyz","pj4","pf9i"};
		DBApp.createTable("do2i2", cols3);
		String tableTrace3 = DBApp.getLastTrace("do2i2");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("do2i2"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"yi6","lo1vi","s8","cm2","p6a"};
		DBApp.createTable("vk", cols4);
		String tableTrace4 = DBApp.getLastTrace("vk");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("vk"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test17TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"pkn","f72","e4f","k3e3x","r07"};
		DBApp.createTable("jqrut", cols0);
		String tableTrace0 = DBApp.getLastTrace("jqrut");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("jqrut"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test18TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"bu","k6v82","ewz","jh6b","wut","lq4b"};
		DBApp.createTable("ufz29", cols0);
		String tableTrace0 = DBApp.getLastTrace("ufz29");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("ufz29"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test19TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"xx3e","hnm","w4j"};
		DBApp.createTable("g5", cols0);
		String tableTrace0 = DBApp.getLastTrace("g5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("g5"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"l9","g7hg4","v6d","ajd","x5k","h5d","rux0"};
		DBApp.createTable("p8o2q", cols1);
		String tableTrace1 = DBApp.getLastTrace("p8o2q");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("p8o2q"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"p3fhj","t1oi"};
		DBApp.createTable("w6f", cols2);
		String tableTrace2 = DBApp.getLastTrace("w6f");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("w6f"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"snh89","d90"};
		DBApp.createTable("tq9", cols3);
		String tableTrace3 = DBApp.getLastTrace("tq9");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("tq9"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"c88","pxf","v2se1","t3j","wj","llag","az59u","fa"};
		DBApp.createTable("au", cols4);
		String tableTrace4 = DBApp.getLastTrace("au");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("au"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test20TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"v17","kk","b6r","xqpso","j2d","ar","x2t","m46","t58l","jqt"};
		DBApp.createTable("s1z", cols0);
		String tableTrace0 = DBApp.getLastTrace("s1z");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("s1z"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"u7r8","a0"};
		DBApp.createTable("k1", cols1);
		String tableTrace1 = DBApp.getLastTrace("k1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("k1"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"l2lvk","xw79t","ls8ow","lo8fu","tf","i83","hyh","ic9"};
		DBApp.createTable("h093p", cols2);
		String tableTrace2 = DBApp.getLastTrace("h093p");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("h093p"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test21TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"gn6","en","r28","aei","a7v4","b0s2","q3c","h94g"};
		DBApp.createTable("j7677", cols0);
		String tableTrace0 = DBApp.getLastTrace("j7677");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("j7677"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"za52","fn2","lf","ye","ie","t641"};
		DBApp.createTable("r0n62", cols1);
		String tableTrace1 = DBApp.getLastTrace("r0n62");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("r0n62"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"fr24","z76","f2p1l"};
		DBApp.createTable("znpib", cols2);
		String tableTrace2 = DBApp.getLastTrace("znpib");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("znpib"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"e97","c71","pp","y8l3z","ebmox"};
		DBApp.createTable("sf743", cols3);
		String tableTrace3 = DBApp.getLastTrace("sf743");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("sf743"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test22TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"y13","e0w21","yxmc","u749b","e46","ow","go48"};
		DBApp.createTable("w85l", cols0);
		String tableTrace0 = DBApp.getLastTrace("w85l");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("w85l"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"js","s0"};
		DBApp.createTable("n3s1", cols1);
		String tableTrace1 = DBApp.getLastTrace("n3s1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("n3s1"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"wyrsq","e0t","ld","c2","dzi02","be2"};
		DBApp.createTable("vcir", cols2);
		String tableTrace2 = DBApp.getLastTrace("vcir");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("vcir"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"o5","t9b","ahh","s82","on3k","dx","g3p","x9nu","z3w7","onh"};
		DBApp.createTable("sz5", cols3);
		String tableTrace3 = DBApp.getLastTrace("sz5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("sz5"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test23TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"eqn","yl00"};
		DBApp.createTable("z8g", cols0);
		String tableTrace0 = DBApp.getLastTrace("z8g");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("z8g"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test24TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 2;
		FileManager.reset();
	
		String[] cols0 = {"qvs","tiyy7","n5j0s"};
		DBApp.createTable("s0a", cols0);
		String tableTrace0 = DBApp.getLastTrace("s0a");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("s0a"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test25TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"nf9","n6t","y46p","m4","su2","v13","d0t1"};
		DBApp.createTable("kn", cols0);
		String tableTrace0 = DBApp.getLastTrace("kn");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("kn"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test26TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"g5"};
		DBApp.createTable("hg22", cols0);
		String tableTrace0 = DBApp.getLastTrace("hg22");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("hg22"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"r39","k87r","mr8","xbtmc","o5l","gld5","zj2by","qp5","f024"};
		DBApp.createTable("gi1", cols1);
		String tableTrace1 = DBApp.getLastTrace("gi1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("gi1"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"akj3","iw6","jel"};
		DBApp.createTable("xc", cols2);
		String tableTrace2 = DBApp.getLastTrace("xc");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("xc"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"gq","m11y","xwqcv","nqb","lrj","c8y","h3","wqvly","w3448"};
		DBApp.createTable("c4r", cols3);
		String tableTrace3 = DBApp.getLastTrace("c4r");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("c4r"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"xpp1f","o1b","xw2m","psl8","tfw5t","kwf","eh2b","us"};
		DBApp.createTable("w1s8", cols4);
		String tableTrace4 = DBApp.getLastTrace("w1s8");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("w1s8"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test27TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"nxx","ih27"};
		DBApp.createTable("i51", cols0);
		String tableTrace0 = DBApp.getLastTrace("i51");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("i51"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"e5","hjp","c680","o99","v11l0"};
		DBApp.createTable("rg4u", cols1);
		String tableTrace1 = DBApp.getLastTrace("rg4u");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("rg4u"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"wk1","vp964","ou2"};
		DBApp.createTable("c7", cols2);
		String tableTrace2 = DBApp.getLastTrace("c7");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("c7"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"cw","uhq90","p2","eu","y496","t266","m12g","n21"};
		DBApp.createTable("p28", cols3);
		String tableTrace3 = DBApp.getLastTrace("p28");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("p28"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test28TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"nf0","vz","c7","ji","ia6","my5l","egsw","f5","p9suu","p07"};
		DBApp.createTable("lscu7", cols0);
		String tableTrace0 = DBApp.getLastTrace("lscu7");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("lscu7"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test29TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"v70","wf06","r2q","qw0"};
		DBApp.createTable("gi", cols0);
		String tableTrace0 = DBApp.getLastTrace("gi");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("gi"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test30TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"dqwg","shq","r86","u57","edrk","yf0n","o276i","s4n","xelie"};
		DBApp.createTable("rs8x", cols0);
		String tableTrace0 = DBApp.getLastTrace("rs8x");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("rs8x"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"rb"};
		DBApp.createTable("c6", cols1);
		String tableTrace1 = DBApp.getLastTrace("c6");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("c6"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"rqx","t4jh","z0"};
		DBApp.createTable("o7vx", cols2);
		String tableTrace2 = DBApp.getLastTrace("o7vx");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("o7vx"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"o3z3","kn098","e7","z33","xwv1t","s73","ohj8t"};
		DBApp.createTable("gq7", cols3);
		String tableTrace3 = DBApp.getLastTrace("gq7");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("gq7"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"b31j","eb6","rpm14","t33la","n6a6","n2byr","bnxi9","u7d5"};
		DBApp.createTable("go", cols4);
		String tableTrace4 = DBApp.getLastTrace("go");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("go"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test31TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"y4v","c0","y643","jqr2"};
		DBApp.createTable("gy5hc", cols0);
		String tableTrace0 = DBApp.getLastTrace("gy5hc");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("gy5hc"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"dbw","u0g","y54kj","uj4m1","kcx","qi"};
		DBApp.createTable("j4310", cols1);
		String tableTrace1 = DBApp.getLastTrace("j4310");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("j4310"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"sy","d73","t70","f0c5n","vi","i683","xs","dck","jo24t","z4i3"};
		DBApp.createTable("q1f", cols2);
		String tableTrace2 = DBApp.getLastTrace("q1f");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("q1f"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"b243","aaj89","xkc2l","ib7","dnig","z2","se","f0","f9z0","h84l1"};
		DBApp.createTable("xgh8", cols3);
		String tableTrace3 = DBApp.getLastTrace("xgh8");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("xgh8"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test32TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"a6m3","ww2","hd7","r8"};
		DBApp.createTable("bsm", cols0);
		String tableTrace0 = DBApp.getLastTrace("bsm");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("bsm"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"sy1u7","u1p0","xk","u5b","xl33","yf2","u2dq","gy4"};
		DBApp.createTable("o3q6", cols1);
		String tableTrace1 = DBApp.getLastTrace("o3q6");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("o3q6"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"y9z"};
		DBApp.createTable("p4l9v", cols2);
		String tableTrace2 = DBApp.getLastTrace("p4l9v");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("p4l9v"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"wf4","mc6","m97","g7dc2","eh5fi","j9"};
		DBApp.createTable("ik8rp", cols3);
		String tableTrace3 = DBApp.getLastTrace("ik8rp");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("ik8rp"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test33TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"orf","z85e","q9mw5","px7"};
		DBApp.createTable("ovm", cols0);
		String tableTrace0 = DBApp.getLastTrace("ovm");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("ovm"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"jh8v","zpe8","i3y","j9g"};
		DBApp.createTable("gawm7", cols1);
		String tableTrace1 = DBApp.getLastTrace("gawm7");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("gawm7"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"q298","yb1"};
		DBApp.createTable("ff1un", cols2);
		String tableTrace2 = DBApp.getLastTrace("ff1un");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("ff1un"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test34TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"f710s","s3t2","ujo","a8di","rl7z","q58","dxv","s34"};
		DBApp.createTable("d60", cols0);
		String tableTrace0 = DBApp.getLastTrace("d60");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("d60"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test35TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"b48","rg","il57","eyr","p7e","v4k7e","hh","g21"};
		DBApp.createTable("uv3", cols0);
		String tableTrace0 = DBApp.getLastTrace("uv3");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("uv3"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"ph404","cwom","zk19f","roc04","fux","f15qp"};
		DBApp.createTable("cq", cols1);
		String tableTrace1 = DBApp.getLastTrace("cq");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("cq"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test36TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"c0e4","rx5d"};
		DBApp.createTable("f0ho1", cols0);
		String tableTrace0 = DBApp.getLastTrace("f0ho1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("f0ho1"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"k4y","g1up","l6","x2","l0j49","nf9","l4","ys1"};
		DBApp.createTable("lz1o", cols1);
		String tableTrace1 = DBApp.getLastTrace("lz1o");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("lz1o"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test37TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"tw","f0","l8s","uw3","ln958","lg1bj","tf"};
		DBApp.createTable("pdk8", cols0);
		String tableTrace0 = DBApp.getLastTrace("pdk8");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("pdk8"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"dst"};
		DBApp.createTable("z8uho", cols1);
		String tableTrace1 = DBApp.getLastTrace("z8uho");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("z8uho"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"g4"};
		DBApp.createTable("wx5", cols2);
		String tableTrace2 = DBApp.getLastTrace("wx5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("wx5"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"r9w","c7","jab","y6","j8","m23","eot","c13t"};
		DBApp.createTable("n26b", cols3);
		String tableTrace3 = DBApp.getLastTrace("n26b");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("n26b"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test38TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"e161g","x94gm"};
		DBApp.createTable("u9", cols0);
		String tableTrace0 = DBApp.getLastTrace("u9");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("u9"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"cfzgg","dvt7","b8w3j","b4ih2"};
		DBApp.createTable("f12qw", cols1);
		String tableTrace1 = DBApp.getLastTrace("f12qw");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("f12qw"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"k6c","v4","t30","y5"};
		DBApp.createTable("cibh0", cols2);
		String tableTrace2 = DBApp.getLastTrace("cibh0");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("cibh0"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"xow","mil02","dn0","kk"};
		DBApp.createTable("j6", cols3);
		String tableTrace3 = DBApp.getLastTrace("j6");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("j6"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test39TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"m043","z5","m2o","wj6","t2o"};
		DBApp.createTable("de", cols0);
		String tableTrace0 = DBApp.getLastTrace("de");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("de"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"su53","kn","d83w"};
		DBApp.createTable("b163", cols1);
		String tableTrace1 = DBApp.getLastTrace("b163");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("b163"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"cl","z4h","rp","wj0","on7","n7361","b0c","p65r","wg2","gu14"};
		DBApp.createTable("k0", cols2);
		String tableTrace2 = DBApp.getLastTrace("k0");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("k0"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"u8","ux"};
		DBApp.createTable("ocp", cols3);
		String tableTrace3 = DBApp.getLastTrace("ocp");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("ocp"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test40TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"zjs","o9007","aky","a60","fl8","x2d"};
		DBApp.createTable("qg", cols0);
		String tableTrace0 = DBApp.getLastTrace("qg");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("qg"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"q56f3","x44o9","v5l"};
		DBApp.createTable("qf0", cols1);
		String tableTrace1 = DBApp.getLastTrace("qf0");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("qf0"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test41TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"pq","qil","gcpo1","pm91","k9k","dkv","yns6"};
		DBApp.createTable("fqu2", cols0);
		String tableTrace0 = DBApp.getLastTrace("fqu2");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("fqu2"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"m61","m5m","dr","is9n","oh7","rm9n","dnce","x7mzt"};
		DBApp.createTable("h313f", cols1);
		String tableTrace1 = DBApp.getLastTrace("h313f");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("h313f"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"oz04v","izn"};
		DBApp.createTable("gg", cols2);
		String tableTrace2 = DBApp.getLastTrace("gg");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("gg"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"eo7b8","b7fu","eo","bqo","lbm9","u28u5","x0v","jqx","ix5f"};
		DBApp.createTable("o5h", cols3);
		String tableTrace3 = DBApp.getLastTrace("o5h");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("o5h"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"tl8"};
		DBApp.createTable("h086x", cols4);
		String tableTrace4 = DBApp.getLastTrace("h086x");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("h086x"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test42TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"okl3","aiw","rx5","u9nk8","a17","sr","ze8","vr32","r5wm"};
		DBApp.createTable("u71", cols0);
		String tableTrace0 = DBApp.getLastTrace("u71");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("u71"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"i0k3","foi3","khs","p3d","mid7","ljq3","hmd"};
		DBApp.createTable("gx6f9", cols1);
		String tableTrace1 = DBApp.getLastTrace("gx6f9");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("gx6f9"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test43TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"el52"};
		DBApp.createTable("big", cols0);
		String tableTrace0 = DBApp.getLastTrace("big");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("big"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test44TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"th2ok"};
		DBApp.createTable("rgo3d", cols0);
		String tableTrace0 = DBApp.getLastTrace("rgo3d");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("rgo3d"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"piej","kd4","n624","gr81","z27","s7a","k6r7w","gt0y","lk"};
		DBApp.createTable("dylvc", cols1);
		String tableTrace1 = DBApp.getLastTrace("dylvc");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("dylvc"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"y1","rht7","v62","pwx6","zz1","c1","p5z"};
		DBApp.createTable("jby9y", cols2);
		String tableTrace2 = DBApp.getLastTrace("jby9y");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("jby9y"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"tej","wa960","g1","mo","b7","p138","mn","p6h4l"};
		DBApp.createTable("m2g5", cols3);
		String tableTrace3 = DBApp.getLastTrace("m2g5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("m2g5"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test45TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"cdot","zh028","n070"};
		DBApp.createTable("ay", cols0);
		String tableTrace0 = DBApp.getLastTrace("ay");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("ay"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"t7u","c5r0","ro59","ut2t","cswl","ykh","s3","gk9","qxbk"};
		DBApp.createTable("jj", cols1);
		String tableTrace1 = DBApp.getLastTrace("jj");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("jj"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"vmk","brq","f7l","s09","o3mn0","i2y"};
		DBApp.createTable("k38rm", cols2);
		String tableTrace2 = DBApp.getLastTrace("k38rm");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("k38rm"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"wqw18","rki"};
		DBApp.createTable("b82", cols3);
		String tableTrace3 = DBApp.getLastTrace("b82");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("b82"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"z0rd","xk3","p3j","a7ktd"};
		DBApp.createTable("lavpk", cols4);
		String tableTrace4 = DBApp.getLastTrace("lavpk");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("lavpk"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test46TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"c498","ia","t9"};
		DBApp.createTable("i1n5", cols0);
		String tableTrace0 = DBApp.getLastTrace("i1n5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("i1n5"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"q1t","fc8","fn","hm3g","jk1c","u8","t6j0","i9f4","yi2","k1"};
		DBApp.createTable("h81d", cols1);
		String tableTrace1 = DBApp.getLastTrace("h81d");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("h81d"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"dr","j5","ds5","n36o3","e647n","v41ee"};
		DBApp.createTable("lc0", cols2);
		String tableTrace2 = DBApp.getLastTrace("lc0");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("lc0"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"qa","lve","f9","l8is8","cl39","tf11"};
		DBApp.createTable("m12", cols3);
		String tableTrace3 = DBApp.getLastTrace("m12");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("m12"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"r66","mrk","b2","mf31m","u6o66","gi7","x7","s618"};
		DBApp.createTable("i7", cols4);
		String tableTrace4 = DBApp.getLastTrace("i7");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("i7"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test47TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"jm6","k3qp","g5","mm4x","mjs","k1q","dy121","ic3tu"};
		DBApp.createTable("qz06z", cols0);
		String tableTrace0 = DBApp.getLastTrace("qz06z");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("qz06z"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"zb8","l03"};
		DBApp.createTable("m34", cols1);
		String tableTrace1 = DBApp.getLastTrace("m34");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("m34"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"jfq","h6u","c4","wp","c1f","c59","og6","l02cb","hhw"};
		DBApp.createTable("xh9", cols2);
		String tableTrace2 = DBApp.getLastTrace("xh9");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("xh9"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test48TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 3;
		FileManager.reset();
	
		String[] cols0 = {"j45","cn","h21","v0n","g7s5g","zl9mk","n20q","xi197","l60"};
		DBApp.createTable("hus4", cols0);
		String tableTrace0 = DBApp.getLastTrace("hus4");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("hus4"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"a98","i59","lwb2"};
		DBApp.createTable("t12", cols1);
		String tableTrace1 = DBApp.getLastTrace("t12");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("t12"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"j1","msc3","z21","yt","q9l","jg16w","op365"};
		DBApp.createTable("s1", cols2);
		String tableTrace2 = DBApp.getLastTrace("s1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("s1"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"s2","lh1","c4oi","lg","sa7","s75a","w88","nb1","qc9i","bu"};
		DBApp.createTable("sgc", cols3);
		String tableTrace3 = DBApp.getLastTrace("sgc");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("sgc"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test49TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"w55e","qku","g681m","z883","w6qe5","n2foe","p98t","vuv"};
		DBApp.createTable("d4", cols0);
		String tableTrace0 = DBApp.getLastTrace("d4");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("d4"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test50TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"l9tn","zsm"};
		DBApp.createTable("w3", cols0);
		String tableTrace0 = DBApp.getLastTrace("w3");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("w3"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test51TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"s7q","y2","d6eh","vh","d5a","yv","c3a","fq3i"};
		DBApp.createTable("u10", cols0);
		String tableTrace0 = DBApp.getLastTrace("u10");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("u10"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"ygx","m39a4","gh","ajxk","gj2h","n2f1","n05","g6k","e0h1n"};
		DBApp.createTable("j7s", cols1);
		String tableTrace1 = DBApp.getLastTrace("j7s");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("j7s"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"x967","deu","f78","g00","xm7"};
		DBApp.createTable("p8", cols2);
		String tableTrace2 = DBApp.getLastTrace("p8");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("p8"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test52TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"k0y","w0b","pb9","q1","vaa","l02n","bj","v2z95","tio","zf"};
		DBApp.createTable("ec", cols0);
		String tableTrace0 = DBApp.getLastTrace("ec");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("ec"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"p66","qo","u4k7","y5","b664","m7nw"};
		DBApp.createTable("a6l", cols1);
		String tableTrace1 = DBApp.getLastTrace("a6l");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("a6l"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test53TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"s1","df40"};
		DBApp.createTable("t8fs", cols0);
		String tableTrace0 = DBApp.getLastTrace("t8fs");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("t8fs"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"ofy","c805","r9","x38z","c6u","b70","eb23","dj","j89y","u30"};
		DBApp.createTable("g40", cols1);
		String tableTrace1 = DBApp.getLastTrace("g40");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("g40"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"uy9"};
		DBApp.createTable("d5", cols2);
		String tableTrace2 = DBApp.getLastTrace("d5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("d5"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test54TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"gqzy","tl","r2d6","c9","r5","r7","x8q","i24s7","vle9p","yn8f3"};
		DBApp.createTable("p3263", cols0);
		String tableTrace0 = DBApp.getLastTrace("p3263");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("p3263"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"pq565","l4u8h","c1c9","u3","cms","e1","gzu","axvv","i0"};
		DBApp.createTable("zm7", cols1);
		String tableTrace1 = DBApp.getLastTrace("zm7");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("zm7"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"a95cx","e3","in240","i7zr","sx","n1i8","ciyv1","uap","xr06"};
		DBApp.createTable("y2g", cols2);
		String tableTrace2 = DBApp.getLastTrace("y2g");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("y2g"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"n0lr"};
		DBApp.createTable("ci2", cols3);
		String tableTrace3 = DBApp.getLastTrace("ci2");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("ci2"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test55TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"q76","f7u","ob"};
		DBApp.createTable("te5", cols0);
		String tableTrace0 = DBApp.getLastTrace("te5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("te5"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test56TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"o34f","f63"};
		DBApp.createTable("kpl09", cols0);
		String tableTrace0 = DBApp.getLastTrace("kpl09");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("kpl09"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"p0z","u7t","si","u7","i6r95"};
		DBApp.createTable("ww0", cols1);
		String tableTrace1 = DBApp.getLastTrace("ww0");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("ww0"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"f1i7","aiaq8","p70a","jz0","u76"};
		DBApp.createTable("i6f", cols2);
		String tableTrace2 = DBApp.getLastTrace("i6f");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("i6f"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"ob0","dd79","a1lh","oqb","zcyj","s7","f3vyi","zuz"};
		DBApp.createTable("m2", cols3);
		String tableTrace3 = DBApp.getLastTrace("m2");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("m2"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test57TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"kl2ee","h6s42","t5j5","gn","xxs","u6u","s90","k4"};
		DBApp.createTable("jw7", cols0);
		String tableTrace0 = DBApp.getLastTrace("jw7");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("jw7"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"yqp","jp3","kvxp","xo0t7","m2w","j60","j2e20","u1a"};
		DBApp.createTable("u5w", cols1);
		String tableTrace1 = DBApp.getLastTrace("u5w");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("u5w"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"ir56q","cmas","xa0j3","jeq55","kzx","w0e0","xl14a","f0p","bvi38","f3vtu"};
		DBApp.createTable("t19", cols2);
		String tableTrace2 = DBApp.getLastTrace("t19");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("t19"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"pa52","b64","hy7","p90","tc67k","h8z","ct0"};
		DBApp.createTable("ck9i", cols3);
		String tableTrace3 = DBApp.getLastTrace("ck9i");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("ck9i"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"w2d","gf0","o8","f6m","ow","s7x","u7v","fe","fd8jq"};
		DBApp.createTable("dt5", cols4);
		String tableTrace4 = DBApp.getLastTrace("dt5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("dt5"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test58TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"o5l82"};
		DBApp.createTable("sr", cols0);
		String tableTrace0 = DBApp.getLastTrace("sr");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("sr"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"aiwdj"};
		DBApp.createTable("u3d", cols1);
		String tableTrace1 = DBApp.getLastTrace("u3d");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("u3d"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"qi0ps","xh","s6m","f3","xx","w2ed","y8","ijeeo"};
		DBApp.createTable("eh3", cols2);
		String tableTrace2 = DBApp.getLastTrace("eh3");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("eh3"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"vtc","vr5"};
		DBApp.createTable("i6i", cols3);
		String tableTrace3 = DBApp.getLastTrace("i6i");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("i6i"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"dl","n4712","x7ba0","mtwl","v3wp6","k98","qnc","egx","v1180"};
		DBApp.createTable("psv", cols4);
		String tableTrace4 = DBApp.getLastTrace("psv");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("psv"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test59TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"o96","ab8","z0","nr","m37v8"};
		DBApp.createTable("i3", cols0);
		String tableTrace0 = DBApp.getLastTrace("i3");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("i3"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"g4k","xg935"};
		DBApp.createTable("u91t", cols1);
		String tableTrace1 = DBApp.getLastTrace("u91t");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("u91t"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test60TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"pr","zhp","cht","py8vu","b59w","lm","so32"};
		DBApp.createTable("cc5b8", cols0);
		String tableTrace0 = DBApp.getLastTrace("cc5b8");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("cc5b8"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"v40k0","j5","q45","h56q","t2"};
		DBApp.createTable("e22ly", cols1);
		String tableTrace1 = DBApp.getLastTrace("e22ly");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("e22ly"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"c3","xy","je192","ku3o","vp","d0h","tt96","kg4"};
		DBApp.createTable("uu132", cols2);
		String tableTrace2 = DBApp.getLastTrace("uu132");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("uu132"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"p9","w35","i3"};
		DBApp.createTable("ubkgk", cols3);
		String tableTrace3 = DBApp.getLastTrace("ubkgk");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("ubkgk"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test61TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"wz39","g4au","qq","ah","g7g4b","j7ij","v8u","ede"};
		DBApp.createTable("tc", cols0);
		String tableTrace0 = DBApp.getLastTrace("tc");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("tc"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test62TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"h6p"};
		DBApp.createTable("i0788", cols0);
		String tableTrace0 = DBApp.getLastTrace("i0788");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("i0788"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"xp0lq","ps4c2","b7d"};
		DBApp.createTable("q4", cols1);
		String tableTrace1 = DBApp.getLastTrace("q4");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("q4"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"ezpo","gb","w88y","n15h"};
		DBApp.createTable("m91kt", cols2);
		String tableTrace2 = DBApp.getLastTrace("m91kt");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("m91kt"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test63TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"w00","kn9n","w3","m1","sd","il5","m8i","r07","qr2","lv1"};
		DBApp.createTable("lm", cols0);
		String tableTrace0 = DBApp.getLastTrace("lm");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("lm"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"vh","yyp"};
		DBApp.createTable("kwr25", cols1);
		String tableTrace1 = DBApp.getLastTrace("kwr25");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("kwr25"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"h693"};
		DBApp.createTable("ks", cols2);
		String tableTrace2 = DBApp.getLastTrace("ks");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("ks"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test64TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"co5","xh","dkt","lme","f6j4f","th","unx","t2t","n25","mo"};
		DBApp.createTable("qkh", cols0);
		String tableTrace0 = DBApp.getLastTrace("qkh");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("qkh"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"lec","d3","e2q1","w7455","p8z","fn7"};
		DBApp.createTable("l34", cols1);
		String tableTrace1 = DBApp.getLastTrace("l34");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("l34"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"qcv","sy1","hg2","ao47","aa0","c8n","e4","n2"};
		DBApp.createTable("t9dzi", cols2);
		String tableTrace2 = DBApp.getLastTrace("t9dzi");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("t9dzi"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"gz1x","k13","y6viq","gc627","s5z5"};
		DBApp.createTable("t514", cols3);
		String tableTrace3 = DBApp.getLastTrace("t514");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("t514"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"b9b1s","n7f92","e5p13","g51r","tnpne"};
		DBApp.createTable("l679s", cols4);
		String tableTrace4 = DBApp.getLastTrace("l679s");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("l679s"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test65TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"mw","kqm","dod","q48","xbuy0","utd5k","yfcp","bus","uiwxz","d4g"};
		DBApp.createTable("j33", cols0);
		String tableTrace0 = DBApp.getLastTrace("j33");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("j33"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test66TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"ocvn","i5v","hp7","u8w1","cl","c06","sa","p9k"};
		DBApp.createTable("x45o", cols0);
		String tableTrace0 = DBApp.getLastTrace("x45o");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("x45o"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"s9"};
		DBApp.createTable("rd5", cols1);
		String tableTrace1 = DBApp.getLastTrace("rd5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("rd5"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"j6"};
		DBApp.createTable("d9o", cols2);
		String tableTrace2 = DBApp.getLastTrace("d9o");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("d9o"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"w9","gp","hwj8","qo","a6d"};
		DBApp.createTable("uk77x", cols3);
		String tableTrace3 = DBApp.getLastTrace("uk77x");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("uk77x"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test67TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"vaglo","y4y","v15n","cnnt","j58","cixa6","aa5"};
		DBApp.createTable("jkn2", cols0);
		String tableTrace0 = DBApp.getLastTrace("jkn2");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("jkn2"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"lo7","x7d83","jg85","st9"};
		DBApp.createTable("kgp", cols1);
		String tableTrace1 = DBApp.getLastTrace("kgp");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("kgp"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test68TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"y9i","odv","r6","acr7","dqg","zqw","h905"};
		DBApp.createTable("m8", cols0);
		String tableTrace0 = DBApp.getLastTrace("m8");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("m8"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test69TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"g92","geu","yyo","k4z","g1","ny4","u8","btd","tx2","qqc"};
		DBApp.createTable("r98", cols0);
		String tableTrace0 = DBApp.getLastTrace("r98");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("r98"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"zg4","n0g","kx9","m5s","p5d","s4vx9"};
		DBApp.createTable("q4", cols1);
		String tableTrace1 = DBApp.getLastTrace("q4");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("q4"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"eo6","y7","u7fn"};
		DBApp.createTable("kop", cols2);
		String tableTrace2 = DBApp.getLastTrace("kop");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("kop"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test70TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"m4","scl16","spy","q8v","y3l2","dg"};
		DBApp.createTable("c773", cols0);
		String tableTrace0 = DBApp.getLastTrace("c773");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("c773"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"s23","durg3","g90","f23l6","em6"};
		DBApp.createTable("cv", cols1);
		String tableTrace1 = DBApp.getLastTrace("cv");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("cv"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"k0b","o5l","q8s","p1h","oqk","r50jo","cn4","b0","w5"};
		DBApp.createTable("p0zx", cols2);
		String tableTrace2 = DBApp.getLastTrace("p0zx");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("p0zx"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"nai","rqy3","nh0","ul","lg6","tw","so","yvw","lsn5","rxg1"};
		DBApp.createTable("w2u1", cols3);
		String tableTrace3 = DBApp.getLastTrace("w2u1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("w2u1"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"ya4ti","w8o","c5","b8","b61","with","h399","yki34"};
		DBApp.createTable("w828", cols4);
		String tableTrace4 = DBApp.getLastTrace("w828");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("w828"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test71TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"jg6rv","y11","hz480","b2","k6","jey71"};
		DBApp.createTable("syx", cols0);
		String tableTrace0 = DBApp.getLastTrace("syx");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("syx"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"n0"};
		DBApp.createTable("ropm", cols1);
		String tableTrace1 = DBApp.getLastTrace("ropm");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("ropm"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test72TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 5;
		FileManager.reset();
	
		String[] cols0 = {"s7mw","v6g"};
		DBApp.createTable("km0p", cols0);
		String tableTrace0 = DBApp.getLastTrace("km0p");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("km0p"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test73TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"klf","x8e","wywi","m8f","i5nvh","bwlt"};
		DBApp.createTable("r2v", cols0);
		String tableTrace0 = DBApp.getLastTrace("r2v");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("r2v"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"j5l","w4","q3m6","d3","p5","f16","i32","r4f51","d0yp5"};
		DBApp.createTable("zd27r", cols1);
		String tableTrace1 = DBApp.getLastTrace("zd27r");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("zd27r"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"ik","v7","trt3","cs5","e2p","o29z","o1uq","bca","v1"};
		DBApp.createTable("fe7l", cols2);
		String tableTrace2 = DBApp.getLastTrace("fe7l");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("fe7l"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"g4","f657","wq5"};
		DBApp.createTable("w65x", cols3);
		String tableTrace3 = DBApp.getLastTrace("w65x");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("w65x"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"sb8m","qm1","m11","l9x"};
		DBApp.createTable("r9", cols4);
		String tableTrace4 = DBApp.getLastTrace("r9");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("r9"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test74TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"s1","dc","qb7","m9b5r","k2i","ga4b7"};
		DBApp.createTable("h4f40", cols0);
		String tableTrace0 = DBApp.getLastTrace("h4f40");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("h4f40"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test75TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"wy","a966","y687","mi2v","f2r"};
		DBApp.createTable("j00q", cols0);
		String tableTrace0 = DBApp.getLastTrace("j00q");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("j00q"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"n54","ti4","pm4k"};
		DBApp.createTable("x93", cols1);
		String tableTrace1 = DBApp.getLastTrace("x93");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("x93"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"tc3","r3b","rv","p263"};
		DBApp.createTable("h5", cols2);
		String tableTrace2 = DBApp.getLastTrace("h5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("h5"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"elk94","di0","mgx","ek1ki"};
		DBApp.createTable("qq8el", cols3);
		String tableTrace3 = DBApp.getLastTrace("qq8el");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("qq8el"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"yv6","ng188","t8m3i","x11g","uk07","me6","k2w3","r7h65"};
		DBApp.createTable("hs9", cols4);
		String tableTrace4 = DBApp.getLastTrace("hs9");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("hs9"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test76TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"k0zr","pm8","x21p6","c32","wx"};
		DBApp.createTable("hqg", cols0);
		String tableTrace0 = DBApp.getLastTrace("hqg");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("hqg"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test77TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"np174","lc","uer","u9228","c3y"};
		DBApp.createTable("i3vof", cols0);
		String tableTrace0 = DBApp.getLastTrace("i3vof");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("i3vof"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"i15","b0yfc","t1i","mtn","b93n7","mc61","de43","r9i"};
		DBApp.createTable("cl", cols1);
		String tableTrace1 = DBApp.getLastTrace("cl");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("cl"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"hwgm4","qxr","f6","sq3"};
		DBApp.createTable("w3qh", cols2);
		String tableTrace2 = DBApp.getLastTrace("w3qh");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("w3qh"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"p48y"};
		DBApp.createTable("l00", cols3);
		String tableTrace3 = DBApp.getLastTrace("l00");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("l00"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test78TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"p0","cdwg","r824h"};
		DBApp.createTable("k3", cols0);
		String tableTrace0 = DBApp.getLastTrace("k3");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("k3"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"or7","g6","x7e","fp","tcn"};
		DBApp.createTable("o0b", cols1);
		String tableTrace1 = DBApp.getLastTrace("o0b");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("o0b"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"d7y43","zks","fi3","z3sz6","scfx","zd68","doi3"};
		DBApp.createTable("yj", cols2);
		String tableTrace2 = DBApp.getLastTrace("yj");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("yj"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"rys5k","q3m0","gei","f7za5","zr61","u4s","rd0b3","g2r","p828"};
		DBApp.createTable("d3", cols3);
		String tableTrace3 = DBApp.getLastTrace("d3");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("d3"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test79TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"a3x","o3"};
		DBApp.createTable("vp", cols0);
		String tableTrace0 = DBApp.getLastTrace("vp");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("vp"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test80TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"tyu","w9","bog6","y7bw","bd5","g06","c049","qv"};
		DBApp.createTable("ul", cols0);
		String tableTrace0 = DBApp.getLastTrace("ul");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("ul"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"u5","k8","wty0","g26","qqq","v8e","u4","vf68","k4","u8"};
		DBApp.createTable("jl51u", cols1);
		String tableTrace1 = DBApp.getLastTrace("jl51u");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("jl51u"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"p2","bav6w","bo1ik","a39nw","wry1h","vo","p49","b1q83","zg"};
		DBApp.createTable("e6k", cols2);
		String tableTrace2 = DBApp.getLastTrace("e6k");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("e6k"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test81TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"ghx94","bza","m58","z8sco"};
		DBApp.createTable("td0", cols0);
		String tableTrace0 = DBApp.getLastTrace("td0");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("td0"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"ok7","pl0","b5vy","f7","r5z6","aw","y08","f68n2","h0y"};
		DBApp.createTable("jv6", cols1);
		String tableTrace1 = DBApp.getLastTrace("jv6");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("jv6"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test82TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"r24","v8","x2v","l7","n7c","d58"};
		DBApp.createTable("ya85", cols0);
		String tableTrace0 = DBApp.getLastTrace("ya85");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("ya85"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"v14rx","i14","pk0u","wr","m0z","cuf"};
		DBApp.createTable("l76r", cols1);
		String tableTrace1 = DBApp.getLastTrace("l76r");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("l76r"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"oqcp","q524","o2v","bh1"};
		DBApp.createTable("nk2", cols2);
		String tableTrace2 = DBApp.getLastTrace("nk2");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("nk2"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"r5","coq","m1il","i3","he","pah","lf39","s63","ohl","ol"};
		DBApp.createTable("bb6e2", cols3);
		String tableTrace3 = DBApp.getLastTrace("bb6e2");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("bb6e2"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test83TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"lj","jl05","c1967"};
		DBApp.createTable("y9n8", cols0);
		String tableTrace0 = DBApp.getLastTrace("y9n8");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("y9n8"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"ckh7z"};
		DBApp.createTable("c12", cols1);
		String tableTrace1 = DBApp.getLastTrace("c12");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("c12"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"r963t","zjm","ahq","dj6","n9f","g0364","ph6","h3i1","gbu"};
		DBApp.createTable("apx", cols2);
		String tableTrace2 = DBApp.getLastTrace("apx");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("apx"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"dj7iu","igy70","h4","q69","v7","a9b","s7oey","n8a","tjv"};
		DBApp.createTable("eoa", cols3);
		String tableTrace3 = DBApp.getLastTrace("eoa");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("eoa"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"gbwv5","esx9","i6w8","x2","ghr","x148","flk"};
		DBApp.createTable("qf4kq", cols4);
		String tableTrace4 = DBApp.getLastTrace("qf4kq");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("qf4kq"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test84TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"y16","o9"};
		DBApp.createTable("u4", cols0);
		String tableTrace0 = DBApp.getLastTrace("u4");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("u4"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"sl928","xv11","b63u","s20zo","h4","upq"};
		DBApp.createTable("dc", cols1);
		String tableTrace1 = DBApp.getLastTrace("dc");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("dc"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"og4","x0kf","luk2","n81","g4869"};
		DBApp.createTable("yp9z", cols2);
		String tableTrace2 = DBApp.getLastTrace("yp9z");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("yp9z"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test85TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"h76"};
		DBApp.createTable("m14", cols0);
		String tableTrace0 = DBApp.getLastTrace("m14");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("m14"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"b139","t7h","gy1"};
		DBApp.createTable("n7", cols1);
		String tableTrace1 = DBApp.getLastTrace("n7");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("n7"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test86TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"t24","zki","vj222"};
		DBApp.createTable("j96", cols0);
		String tableTrace0 = DBApp.getLastTrace("j96");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("j96"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"k20g6","cuyw9","hy9ma","s9l"};
		DBApp.createTable("lr", cols1);
		String tableTrace1 = DBApp.getLastTrace("lr");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("lr"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test87TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"yp2"};
		DBApp.createTable("t8g3", cols0);
		String tableTrace0 = DBApp.getLastTrace("t8g3");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("t8g3"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"pth5a","bz9","w32","hqv9","d92y0","vm7"};
		DBApp.createTable("bh", cols1);
		String tableTrace1 = DBApp.getLastTrace("bh");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("bh"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"p57","zmd","q1","tjd4","zd2","q7","n1l8"};
		DBApp.createTable("p714", cols2);
		String tableTrace2 = DBApp.getLastTrace("p714");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("p714"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test88TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"g6gh","mbw"};
		DBApp.createTable("fuj", cols0);
		String tableTrace0 = DBApp.getLastTrace("fuj");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("fuj"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"i44","y5","r67","kmk","kphj8","i67"};
		DBApp.createTable("n3", cols1);
		String tableTrace1 = DBApp.getLastTrace("n3");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("n3"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"q355","se2j","q1","z77"};
		DBApp.createTable("mr5", cols2);
		String tableTrace2 = DBApp.getLastTrace("mr5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("mr5"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"f064","tj8","l1ah","o38","xh2","u26"};
		DBApp.createTable("pq4", cols3);
		String tableTrace3 = DBApp.getLastTrace("pq4");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("pq4"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test89TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"z9n"};
		DBApp.createTable("js5", cols0);
		String tableTrace0 = DBApp.getLastTrace("js5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("js5"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"crnwo","x3m"};
		DBApp.createTable("t33", cols1);
		String tableTrace1 = DBApp.getLastTrace("t33");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("t33"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"o101","ib7","zex","uhy","a84nq","cq73"};
		DBApp.createTable("r837", cols2);
		String tableTrace2 = DBApp.getLastTrace("r837");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("r837"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"fx","wb","jg1","n9","gh25k","ab1"};
		DBApp.createTable("x11", cols3);
		String tableTrace3 = DBApp.getLastTrace("x11");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("x11"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test90TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"y94en","pcp","r3h9z","j5ac","f1e0","b48","et6f"};
		DBApp.createTable("l2", cols0);
		String tableTrace0 = DBApp.getLastTrace("l2");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("l2"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"evu"};
		DBApp.createTable("cv637", cols1);
		String tableTrace1 = DBApp.getLastTrace("cv637");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("cv637"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"yw39","w92","e300","mc111","io8","zc","x4b7"};
		DBApp.createTable("u5gxq", cols2);
		String tableTrace2 = DBApp.getLastTrace("u5gxq");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("u5gxq"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"t238","d2u46","r0ky","zws","d7","r6t21"};
		DBApp.createTable("rjg", cols3);
		String tableTrace3 = DBApp.getLastTrace("rjg");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("rjg"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"q720","e373","fo7ub"};
		DBApp.createTable("n2y", cols4);
		String tableTrace4 = DBApp.getLastTrace("n2y");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("n2y"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test91TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"c5","ird","l8","c3ce"};
		DBApp.createTable("v83v", cols0);
		String tableTrace0 = DBApp.getLastTrace("v83v");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("v83v"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"p2","l7","ypto","t7ysm","lm787","equ"};
		DBApp.createTable("dn26h", cols1);
		String tableTrace1 = DBApp.getLastTrace("dn26h");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("dn26h"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"x73a0","fq75","b49","o5y","kge","e7l6","h0c","xsf"};
		DBApp.createTable("e3hc", cols2);
		String tableTrace2 = DBApp.getLastTrace("e3hc");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("e3hc"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"fh69","rni2y","a2ra","a852","jmw","wi041","z55","nf3"};
		DBApp.createTable("v09", cols3);
		String tableTrace3 = DBApp.getLastTrace("v09");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("v09"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test92TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"pu2","x8u","ov","b0","wnkgt","o19","u6q","j00","c4aa","nw8"};
		DBApp.createTable("ubu", cols0);
		String tableTrace0 = DBApp.getLastTrace("ubu");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("ubu"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"m53","p5k07","il7l4","b9","w7i"};
		DBApp.createTable("t59", cols1);
		String tableTrace1 = DBApp.getLastTrace("t59");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("t59"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test93TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"o3h80","t32","z72","ss","ae","o8ru","qn1","ek5"};
		DBApp.createTable("c17", cols0);
		String tableTrace0 = DBApp.getLastTrace("c17");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("c17"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"j3yj5","b3che","z0m","it9l","ok","mm065","tga5","l4b","rng9","eq4g"};
		DBApp.createTable("i97x", cols1);
		String tableTrace1 = DBApp.getLastTrace("i97x");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("i97x"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test94TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"ispw","xow","wr71","k573"};
		DBApp.createTable("r41", cols0);
		String tableTrace0 = DBApp.getLastTrace("r41");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("r41"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"kfot","sg7o","is877","zu"};
		DBApp.createTable("nol", cols1);
		String tableTrace1 = DBApp.getLastTrace("nol");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("nol"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test95TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"gat","u2y","k56","my"};
		DBApp.createTable("y8s1", cols0);
		String tableTrace0 = DBApp.getLastTrace("y8s1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("y8s1"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"z6","z9","s65"};
		DBApp.createTable("n3k", cols1);
		String tableTrace1 = DBApp.getLastTrace("n3k");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("n3k"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test96TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"ou","b8g3"};
		DBApp.createTable("hi0", cols0);
		String tableTrace0 = DBApp.getLastTrace("hi0");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("hi0"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test97TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 10;
		FileManager.reset();
	
		String[] cols0 = {"kja3","h8","fyqwo","oygh","z9g","g8v0"};
		DBApp.createTable("w8c", cols0);
		String tableTrace0 = DBApp.getLastTrace("w8c");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("w8c"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"xz35"};
		DBApp.createTable("c8a", cols1);
		String tableTrace1 = DBApp.getLastTrace("c8a");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("c8a"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"q1yy8","z091e","q4p","c8w45","ak5t","k6","d06","tkph"};
		DBApp.createTable("cxb7", cols2);
		String tableTrace2 = DBApp.getLastTrace("cxb7");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("cxb7"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"z96","s4c"};
		DBApp.createTable("w83", cols3);
		String tableTrace3 = DBApp.getLastTrace("w83");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("w83"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"e0s","fa0wv","sh56","ky10b","fn9","nlu","z26kb","y77","xlgi"};
		DBApp.createTable("u6m", cols4);
		String tableTrace4 = DBApp.getLastTrace("u6m");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("u6m"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test98TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"s5987","v41b4","g9bp","r2g","w92","l6","r24co","t0k7","ws"};
		DBApp.createTable("l034", cols0);
		String tableTrace0 = DBApp.getLastTrace("l034");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("l034"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test99TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"hx9","w38","v30gr","n3e","h04","et9","xeb2"};
		DBApp.createTable("giy8", cols0);
		String tableTrace0 = DBApp.getLastTrace("giy8");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("giy8"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"ozn","vl7h4","d4zv2","na"};
		DBApp.createTable("lq", cols1);
		String tableTrace1 = DBApp.getLastTrace("lq");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("lq"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"xp3","al7","a0vj","t3","iht","l067f","r63"};
		DBApp.createTable("j9", cols2);
		String tableTrace2 = DBApp.getLastTrace("j9");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("j9"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"pai","y22x","nsn","e90","ba11","xr9","zv0","f80","tx26","h0"};
		DBApp.createTable("dp1", cols3);
		String tableTrace3 = DBApp.getLastTrace("dp1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("dp1"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"ri4","z0x","k2","e97w7","cu","ty8qj","l697q","rpl"};
		DBApp.createTable("fu", cols4);
		String tableTrace4 = DBApp.getLastTrace("fu");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("fu"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test100TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"e88l","l961d","oc7","c8nis","p6n","oce2"};
		DBApp.createTable("u05", cols0);
		String tableTrace0 = DBApp.getLastTrace("u05");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("u05"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"fc","qy255","jt","zx2d","oh","c3","sk8if","rb"};
		DBApp.createTable("l60", cols1);
		String tableTrace1 = DBApp.getLastTrace("l60");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("l60"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"xxa","isu","ydy5c","sl1e","o283","trkt","srr"};
		DBApp.createTable("b1oj", cols2);
		String tableTrace2 = DBApp.getLastTrace("b1oj");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("b1oj"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test101TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"mk4","us1","r90","a9a","bi","xig"};
		DBApp.createTable("u2", cols0);
		String tableTrace0 = DBApp.getLastTrace("u2");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("u2"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"i3y","ubk"};
		DBApp.createTable("x91", cols1);
		String tableTrace1 = DBApp.getLastTrace("x91");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("x91"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test102TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"i0z0"};
		DBApp.createTable("n51q", cols0);
		String tableTrace0 = DBApp.getLastTrace("n51q");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("n51q"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"k239","x2c82","vj","nzc","h63","e301","af","h83","ykd","hr7"};
		DBApp.createTable("v91", cols1);
		String tableTrace1 = DBApp.getLastTrace("v91");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("v91"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"d27","o11","q59","o85a"};
		DBApp.createTable("agz", cols2);
		String tableTrace2 = DBApp.getLastTrace("agz");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("agz"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"p3of","oj"};
		DBApp.createTable("pl4l", cols3);
		String tableTrace3 = DBApp.getLastTrace("pl4l");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("pl4l"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"e615k","q37l7","s75"};
		DBApp.createTable("q61", cols4);
		String tableTrace4 = DBApp.getLastTrace("q61");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("q61"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test103TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"d25o2","qq","s62","n6","v8221","x60"};
		DBApp.createTable("bu", cols0);
		String tableTrace0 = DBApp.getLastTrace("bu");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("bu"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"z7m58","ht","nfog","ypsea","z81g0"};
		DBApp.createTable("f3p2l", cols1);
		String tableTrace1 = DBApp.getLastTrace("f3p2l");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("f3p2l"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"u66","bnab","r2m7","nz","m2p1","h0","gg92j","oc","zy7","wl3"};
		DBApp.createTable("gu1p0", cols2);
		String tableTrace2 = DBApp.getLastTrace("gu1p0");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("gu1p0"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"j5","r495","o7473"};
		DBApp.createTable("yan7", cols3);
		String tableTrace3 = DBApp.getLastTrace("yan7");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("yan7"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"j80j","yr8","xg3e","r0c","k3","z83","uc2","i2v","u51","e3qf"};
		DBApp.createTable("z2", cols4);
		String tableTrace4 = DBApp.getLastTrace("z2");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("z2"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test104TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"vy2w","nipga","av","g7odc","xj4g","r8j"};
		DBApp.createTable("yaps", cols0);
		String tableTrace0 = DBApp.getLastTrace("yaps");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("yaps"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"c0l","zq4d1","kh"};
		DBApp.createTable("mz", cols1);
		String tableTrace1 = DBApp.getLastTrace("mz");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("mz"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"ykk","yb0","s4gca"};
		DBApp.createTable("yr8", cols2);
		String tableTrace2 = DBApp.getLastTrace("yr8");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("yr8"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test105TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"l7w","sh4c","u2q51","jlwu","tdsl","n9c","ov","n5"};
		DBApp.createTable("d24", cols0);
		String tableTrace0 = DBApp.getLastTrace("d24");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("d24"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"mx5","r4","o11","r3b","nq","k5g","jw2"};
		DBApp.createTable("u61", cols1);
		String tableTrace1 = DBApp.getLastTrace("u61");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("u61"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test106TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"fc9","u73","tz"};
		DBApp.createTable("w8", cols0);
		String tableTrace0 = DBApp.getLastTrace("w8");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("w8"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"yt63v","y3"};
		DBApp.createTable("i6la", cols1);
		String tableTrace1 = DBApp.getLastTrace("i6la");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("i6la"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"b95e","b04","fr1pb","mp5q"};
		DBApp.createTable("u61", cols2);
		String tableTrace2 = DBApp.getLastTrace("u61");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("u61"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"o2","kbdn","x8b","a6x","ad612","z7k","xw93h","hkh","tgx"};
		DBApp.createTable("q4", cols3);
		String tableTrace3 = DBApp.getLastTrace("q4");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("q4"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"drz","d73","ad19"};
		DBApp.createTable("zhb9", cols4);
		String tableTrace4 = DBApp.getLastTrace("zhb9");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("zhb9"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test107TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"p03b","uy09","wl"};
		DBApp.createTable("co", cols0);
		String tableTrace0 = DBApp.getLastTrace("co");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("co"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"h4","j1","bb92a","f77","zl65","jme24"};
		DBApp.createTable("t32", cols1);
		String tableTrace1 = DBApp.getLastTrace("t32");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("t32"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"xr","cu","b2juq","xa5u","u3g"};
		DBApp.createTable("ju4t", cols2);
		String tableTrace2 = DBApp.getLastTrace("ju4t");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("ju4t"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"sb6","s5","zi12"};
		DBApp.createTable("q0l", cols3);
		String tableTrace3 = DBApp.getLastTrace("q0l");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("q0l"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"j4xb","o22","u75n","x7t","g7td","owb","nt3"};
		DBApp.createTable("jy7", cols4);
		String tableTrace4 = DBApp.getLastTrace("jy7");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("jy7"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test108TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"os3","z92","j20tp","i5"};
		DBApp.createTable("nr3e", cols0);
		String tableTrace0 = DBApp.getLastTrace("nr3e");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("nr3e"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"p2","h77"};
		DBApp.createTable("my1", cols1);
		String tableTrace1 = DBApp.getLastTrace("my1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("my1"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"d30","v2643","g4","ux61m","ar7"};
		DBApp.createTable("qai91", cols2);
		String tableTrace2 = DBApp.getLastTrace("qai91");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("qai91"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test109TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"n5b","tx317","w86f","wv2c","m8h","kz99"};
		DBApp.createTable("d856", cols0);
		String tableTrace0 = DBApp.getLastTrace("d856");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("d856"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"n6w","ffh","p474"};
		DBApp.createTable("j0", cols1);
		String tableTrace1 = DBApp.getLastTrace("j0");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("j0"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test110TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"k49","c2i","dk","z134u","g9sdd","g5","ao","vx"};
		DBApp.createTable("al5", cols0);
		String tableTrace0 = DBApp.getLastTrace("al5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("al5"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"fsvm","kd","im5n","d8f","hh022"};
		DBApp.createTable("c4", cols1);
		String tableTrace1 = DBApp.getLastTrace("c4");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("c4"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"p3eq","fs","b10","gy1","t5l","f84i5","g0"};
		DBApp.createTable("o79il", cols2);
		String tableTrace2 = DBApp.getLastTrace("o79il");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("o79il"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"o5ls","ami","i25","g0r","plx","w99","za","c6o"};
		DBApp.createTable("w0l", cols3);
		String tableTrace3 = DBApp.getLastTrace("w0l");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("w0l"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"z74","v6h","tw8","qa1ho"};
		DBApp.createTable("n0", cols4);
		String tableTrace4 = DBApp.getLastTrace("n0");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("n0"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test111TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"q6u"};
		DBApp.createTable("ee", cols0);
		String tableTrace0 = DBApp.getLastTrace("ee");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("ee"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"m0f","m8n","p09h","zmx53"};
		DBApp.createTable("iy", cols1);
		String tableTrace1 = DBApp.getLastTrace("iy");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("iy"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"j73w","fw9","q1i","ugt","g633y","n95","a5m"};
		DBApp.createTable("ksli7", cols2);
		String tableTrace2 = DBApp.getLastTrace("ksli7");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("ksli7"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"pnnp","zy","sg43","jn43h","f5v"};
		DBApp.createTable("qlwp", cols3);
		String tableTrace3 = DBApp.getLastTrace("qlwp");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("qlwp"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test112TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"ts5","nqn"};
		DBApp.createTable("cr96", cols0);
		String tableTrace0 = DBApp.getLastTrace("cr96");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("cr96"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"w7","s750","z2","jhwl","y5b","fia","s5d","svf3","jk5"};
		DBApp.createTable("t6", cols1);
		String tableTrace1 = DBApp.getLastTrace("t6");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("t6"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"tdm"};
		DBApp.createTable("c7i", cols2);
		String tableTrace2 = DBApp.getLastTrace("c7i");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("c7i"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"s2bn","u9j6","m70","d3v"};
		DBApp.createTable("pp", cols3);
		String tableTrace3 = DBApp.getLastTrace("pp");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("pp"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test113TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"kw9","jz"};
		DBApp.createTable("uufm", cols0);
		String tableTrace0 = DBApp.getLastTrace("uufm");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("uufm"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"ac4","ti","z4","os","fh6s2"};
		DBApp.createTable("js", cols1);
		String tableTrace1 = DBApp.getLastTrace("js");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("js"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"v09","j75","f9j","gfy","az","mcz","cj","q5e","l3"};
		DBApp.createTable("rmx", cols2);
		String tableTrace2 = DBApp.getLastTrace("rmx");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("rmx"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test114TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"aa","f63","h32","j4j"};
		DBApp.createTable("ndw0", cols0);
		String tableTrace0 = DBApp.getLastTrace("ndw0");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("ndw0"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"t7gy","h8","s0","dyu4","w1733","u1y","wf7","h9","kxv","m15"};
		DBApp.createTable("gb15", cols1);
		String tableTrace1 = DBApp.getLastTrace("gb15");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("gb15"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"u66","su0h","xcj6y","z35a","q06","v195b","kaudg","f2","s8"};
		DBApp.createTable("mo5", cols2);
		String tableTrace2 = DBApp.getLastTrace("mo5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("mo5"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"vhza8","xd35k","zp","yj36s","s666","o6hoa","rs7"};
		DBApp.createTable("j38", cols3);
		String tableTrace3 = DBApp.getLastTrace("j38");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("j38"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"y74p9","dvm","q78","s8g3i","eg","h1z","au5","h6"};
		DBApp.createTable("ydq", cols4);
		String tableTrace4 = DBApp.getLastTrace("ydq");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("ydq"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test115TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"g91rc","y8h","jv9y","c6","bg79k","wc18","ju2","d4b0","a1"};
		DBApp.createTable("rlr8u", cols0);
		String tableTrace0 = DBApp.getLastTrace("rlr8u");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("rlr8u"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"b582"};
		DBApp.createTable("i9", cols1);
		String tableTrace1 = DBApp.getLastTrace("i9");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("i9"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"ji","c13","y8i9e","pbw","cedvf"};
		DBApp.createTable("zaf23", cols2);
		String tableTrace2 = DBApp.getLastTrace("zaf23");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("zaf23"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"b5t","e0","ub","jg1","fkz","yhv85","d8","l9ed"};
		DBApp.createTable("ub217", cols3);
		String tableTrace3 = DBApp.getLastTrace("ub217");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("ub217"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test116TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"l7","jbmg","d1p","rx","r9sq","zs38h","r16"};
		DBApp.createTable("kq6kk", cols0);
		String tableTrace0 = DBApp.getLastTrace("kq6kk");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("kq6kk"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test117TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"n4xnz"};
		DBApp.createTable("c99", cols0);
		String tableTrace0 = DBApp.getLastTrace("c99");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("c99"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test118TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"sx092"};
		DBApp.createTable("k5p", cols0);
		String tableTrace0 = DBApp.getLastTrace("k5p");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("k5p"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"rox"};
		DBApp.createTable("t56r", cols1);
		String tableTrace1 = DBApp.getLastTrace("t56r");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("t56r"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"snvj","m1","m6","ha9","t99","a00","k9yc1","h4w3"};
		DBApp.createTable("rfj", cols2);
		String tableTrace2 = DBApp.getLastTrace("rfj");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("rfj"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test119TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"p67j","jxq0j","l620","l34","v81","s19","l34f","p8a28","sl9","g0273"};
		DBApp.createTable("e6h1", cols0);
		String tableTrace0 = DBApp.getLastTrace("e6h1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("e6h1"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test120TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"xl63","sso6","j1yx","e6","zh9"};
		DBApp.createTable("k07", cols0);
		String tableTrace0 = DBApp.getLastTrace("k07");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("k07"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"bj815","z4x","u0p","xpo","yfp"};
		DBApp.createTable("rzzvl", cols1);
		String tableTrace1 = DBApp.getLastTrace("rzzvl");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("rzzvl"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"oj8r","j0rp","c5v2","p09","ve","h77"};
		DBApp.createTable("z5", cols2);
		String tableTrace2 = DBApp.getLastTrace("z5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("z5"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"itqj4","wvv9t","jo08p","b534","ddzh","m3","vx1","xau"};
		DBApp.createTable("rl", cols3);
		String tableTrace3 = DBApp.getLastTrace("rl");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("rl"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"at","viaw1","m71","f323","d0"};
		DBApp.createTable("xxh", cols4);
		String tableTrace4 = DBApp.getLastTrace("xxh");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("xxh"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test121TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 20;
		FileManager.reset();
	
		String[] cols0 = {"n85h"};
		DBApp.createTable("ez18", cols0);
		String tableTrace0 = DBApp.getLastTrace("ez18");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("ez18"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test122TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"f7s"};
		DBApp.createTable("z8", cols0);
		String tableTrace0 = DBApp.getLastTrace("z8");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("z8"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"wladi","vr","mj0","e17","x44j7","gve4","eo03","j5e4q","x101"};
		DBApp.createTable("xi", cols1);
		String tableTrace1 = DBApp.getLastTrace("xi");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("xi"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test123TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"it0"};
		DBApp.createTable("qkjj", cols0);
		String tableTrace0 = DBApp.getLastTrace("qkjj");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("qkjj"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"ryim","r18","hs23"};
		DBApp.createTable("t44c", cols1);
		String tableTrace1 = DBApp.getLastTrace("t44c");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("t44c"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"v8","z4g","v5"};
		DBApp.createTable("g509", cols2);
		String tableTrace2 = DBApp.getLastTrace("g509");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("g509"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test124TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"d65","cv2"};
		DBApp.createTable("y3ta5", cols0);
		String tableTrace0 = DBApp.getLastTrace("y3ta5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("y3ta5"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"f8k","s8pix"};
		DBApp.createTable("g1", cols1);
		String tableTrace1 = DBApp.getLastTrace("g1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("g1"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"mx4o","wp5m","n12v","chwzl","tzo63"};
		DBApp.createTable("f8po", cols2);
		String tableTrace2 = DBApp.getLastTrace("f8po");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("f8po"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"l5x","or"};
		DBApp.createTable("o59b", cols3);
		String tableTrace3 = DBApp.getLastTrace("o59b");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("o59b"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test125TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"fsfg","rq2"};
		DBApp.createTable("j450q", cols0);
		String tableTrace0 = DBApp.getLastTrace("j450q");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("j450q"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"bt115"};
		DBApp.createTable("hfk", cols1);
		String tableTrace1 = DBApp.getLastTrace("hfk");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("hfk"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"qgh","j3w94","v4","ee8","kmsp","k39i","prq"};
		DBApp.createTable("u2g", cols2);
		String tableTrace2 = DBApp.getLastTrace("u2g");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("u2g"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test126TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"tj1","rq6","d2553","wu4c","y2","h06s","k01","g3px","sf"};
		DBApp.createTable("yqu5", cols0);
		String tableTrace0 = DBApp.getLastTrace("yqu5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("yqu5"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test127TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"agk","u8o","v37","c6","z9","z7l","d58"};
		DBApp.createTable("gk19k", cols0);
		String tableTrace0 = DBApp.getLastTrace("gk19k");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("gk19k"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"pcrv","t01","lf","tg3"};
		DBApp.createTable("kyx", cols1);
		String tableTrace1 = DBApp.getLastTrace("kyx");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("kyx"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"r1","qu7","lxl","yvee","q047","ypt","k249"};
		DBApp.createTable("ko", cols2);
		String tableTrace2 = DBApp.getLastTrace("ko");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("ko"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test128TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"hxo","z1p78","mp4","u681","kh","b49","f05rk","e306s","k70","e7z21"};
		DBApp.createTable("tanp", cols0);
		String tableTrace0 = DBApp.getLastTrace("tanp");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("tanp"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"op72","yv1i7","t0","u56","htd","i8","s21","s38","n48"};
		DBApp.createTable("n66mk", cols1);
		String tableTrace1 = DBApp.getLastTrace("n66mk");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("n66mk"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test129TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"pla74"};
		DBApp.createTable("q614", cols0);
		String tableTrace0 = DBApp.getLastTrace("q614");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("q614"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"h80p"};
		DBApp.createTable("g7", cols1);
		String tableTrace1 = DBApp.getLastTrace("g7");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("g7"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"f6pi","c52m3","n4l","o1"};
		DBApp.createTable("x8m5d", cols2);
		String tableTrace2 = DBApp.getLastTrace("x8m5d");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("x8m5d"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"a0w25","w8jd4","qx5i5","vr7","tmy"};
		DBApp.createTable("gr", cols3);
		String tableTrace3 = DBApp.getLastTrace("gr");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("gr"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test130TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"o92","q27","xxn2"};
		DBApp.createTable("cx", cols0);
		String tableTrace0 = DBApp.getLastTrace("cx");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("cx"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"j39","u3r","sn","kv","g6"};
		DBApp.createTable("thc2u", cols1);
		String tableTrace1 = DBApp.getLastTrace("thc2u");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("thc2u"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"qev","tt6","zs11j","hn7"};
		DBApp.createTable("m6w", cols2);
		String tableTrace2 = DBApp.getLastTrace("m6w");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("m6w"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"y7f","z3873","kj08","eo","d43j2","w0","tu1i0","m65","y2b"};
		DBApp.createTable("rt7", cols3);
		String tableTrace3 = DBApp.getLastTrace("rt7");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("rt7"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test131TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"bf","ev","nj1","i04","u79"};
		DBApp.createTable("o0", cols0);
		String tableTrace0 = DBApp.getLastTrace("o0");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("o0"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"lxs"};
		DBApp.createTable("u02o", cols1);
		String tableTrace1 = DBApp.getLastTrace("u02o");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("u02o"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"v47","uo4","c40","v8ylw","rfu","o75","yic2"};
		DBApp.createTable("ja1", cols2);
		String tableTrace2 = DBApp.getLastTrace("ja1");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("ja1"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test132TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"oe7uf","aa3","ccc","x943b","i17qd"};
		DBApp.createTable("yece", cols0);
		String tableTrace0 = DBApp.getLastTrace("yece");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("yece"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"q7"};
		DBApp.createTable("m61", cols1);
		String tableTrace1 = DBApp.getLastTrace("m61");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("m61"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"h5","f2","l5a","ox0","k2","vp0"};
		DBApp.createTable("m5", cols2);
		String tableTrace2 = DBApp.getLastTrace("m5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("m5"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"jp79w","wqgi","w5o","ht0","d798","q3x"};
		DBApp.createTable("d7m", cols3);
		String tableTrace3 = DBApp.getLastTrace("d7m");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("d7m"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test133TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"b2","vsbpy","b27","m15d"};
		DBApp.createTable("bf22", cols0);
		String tableTrace0 = DBApp.getLastTrace("bf22");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("bf22"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"qe6y","hq","b4hec","uv040"};
		DBApp.createTable("x10l9", cols1);
		String tableTrace1 = DBApp.getLastTrace("x10l9");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("x10l9"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"raat1","c135","fd57","ia"};
		DBApp.createTable("ny", cols2);
		String tableTrace2 = DBApp.getLastTrace("ny");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("ny"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test134TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"d4","g834","g4","f2","uq","kwc","u5"};
		DBApp.createTable("b2", cols0);
		String tableTrace0 = DBApp.getLastTrace("b2");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("b2"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"i1","slv","ghuy","o1v"};
		DBApp.createTable("v7d", cols1);
		String tableTrace1 = DBApp.getLastTrace("v7d");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("v7d"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"u95","jiyo3","l7","wj4j","al","yy7"};
		DBApp.createTable("c63", cols2);
		String tableTrace2 = DBApp.getLastTrace("c63");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("c63"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"nt0","ny","td","xav","rbl5","yv8","e37gj","xsw34"};
		DBApp.createTable("lo", cols3);
		String tableTrace3 = DBApp.getLastTrace("lo");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("lo"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test135TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"rmy8a","r2","e16v","bf29","zh4l9","uov5j","oz0","ejh","autm5"};
		DBApp.createTable("tqx5", cols0);
		String tableTrace0 = DBApp.getLastTrace("tqx5");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("tqx5"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"gje2n","v81","vb","r7","ih8gj","rl1r","d034b","e8f"};
		DBApp.createTable("he382", cols1);
		String tableTrace1 = DBApp.getLastTrace("he382");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("he382"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"gt21","aas"};
		DBApp.createTable("j8982", cols2);
		String tableTrace2 = DBApp.getLastTrace("j8982");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("j8982"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"u0","j45","iufhn","sds16","s2z","ay6c"};
		DBApp.createTable("y28vp", cols3);
		String tableTrace3 = DBApp.getLastTrace("y28vp");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("y28vp"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test136TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"t838","i3d04","e590","b06"};
		DBApp.createTable("n9k", cols0);
		String tableTrace0 = DBApp.getLastTrace("n9k");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("n9k"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"x13","pi637","x0w0c","u24","lb1","ea"};
		DBApp.createTable("wyc0", cols1);
		String tableTrace1 = DBApp.getLastTrace("wyc0");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("wyc0"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"d05","w0d","lhq","qd","al","eq","p9l7","s8h"};
		DBApp.createTable("sl77i", cols2);
		String tableTrace2 = DBApp.getLastTrace("sl77i");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("sl77i"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"j7x"};
		DBApp.createTable("ja8h", cols3);
		String tableTrace3 = DBApp.getLastTrace("ja8h");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("ja8h"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"p92","oz1r","s9t","q3o"};
		DBApp.createTable("d3j", cols4);
		String tableTrace4 = DBApp.getLastTrace("d3j");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("d3j"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test137TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"x7w4","go7g","my2","i6","j8597","id9","t83u"};
		DBApp.createTable("mz7a3", cols0);
		String tableTrace0 = DBApp.getLastTrace("mz7a3");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("mz7a3"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 10000)
	public void test138TableTraceDBApp() throws Exception
	{
		DBApp.dataPageSize = 50;
		FileManager.reset();
	
		String[] cols0 = {"t3c3b","q15","v9629","xoh","u4s1","ny548","kq4","i04","h8w"};
		DBApp.createTable("xj161", cols0);
		String tableTrace0 = DBApp.getLastTrace("xj161");
		assertTrue("The trace String inside a table should contain its name.", tableTrace0.contains("xj161"));
		for(int i=0;i<cols0.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace0.contains(cols0[i]));
		}
		String[] cols1 = {"et224","vk","cn","d4d","u1s","dxw0","lc1","s1w8","zr812"};
		DBApp.createTable("as8", cols1);
		String tableTrace1 = DBApp.getLastTrace("as8");
		assertTrue("The trace String inside a table should contain its name.", tableTrace1.contains("as8"));
		for(int i=0;i<cols1.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace1.contains(cols1[i]));
		}
		String[] cols2 = {"zl6a","d1f9p","d059","rmu","ir4","vz2"};
		DBApp.createTable("m82", cols2);
		String tableTrace2 = DBApp.getLastTrace("m82");
		assertTrue("The trace String inside a table should contain its name.", tableTrace2.contains("m82"));
		for(int i=0;i<cols2.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace2.contains(cols2[i]));
		}
		String[] cols3 = {"lo2","r1x","w3819","gfu"};
		DBApp.createTable("th", cols3);
		String tableTrace3 = DBApp.getLastTrace("th");
		assertTrue("The trace String inside a table should contain its name.", tableTrace3.contains("th"));
		for(int i=0;i<cols3.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace3.contains(cols3[i]));
		}
		String[] cols4 = {"v1do"};
		DBApp.createTable("wcm6", cols4);
		String tableTrace4 = DBApp.getLastTrace("wcm6");
		assertTrue("The trace String inside a table should contain its name.", tableTrace4.contains("wcm6"));
		for(int i=0;i<cols4.length;i++)
		{
			assertTrue("The trace String inside a table should contain its columns names.", tableTrace4.contains(cols4[i]));
		}
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(267);
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