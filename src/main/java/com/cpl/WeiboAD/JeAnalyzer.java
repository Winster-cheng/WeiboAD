//package com.cpl.WeiboAD;
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileOutputStream;
//import java.io.InputStreamReader;
//import java.io.OutputStreamWriter;
//import java.io.Reader;
//import java.io.StringReader;
//import java.io.Writer;
//
//import org.apache.lucene.analysis.*;
//import org.mira.lucene.analysis.MIK_CAnalyzer;
///*
// *Create by winstercheng 2017年04月17日 
// *this class have data change from (WeiboID MESSAGE) ->(weiboid key1,key2,key3)
// */
//
//public class JeAnalyzer {
//
//
//	//真正实现分词的方法
//	public static String transJe(String testString, String c1, String c2) {
//		String result = "";
//		try {
//			Analyzer analyzer = new MIK_CAnalyzer();
//			Reader r = new StringReader(testString);
//			TokenStream ts = (TokenStream) analyzer.tokenStream("", r);//核心语句
//			Token t;
//			while ((t = ts.next()) != null) {
//				result += t.termText() + ",";
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return result;
//	}
//
//	//这个方法实现输入 WeiboID  message  -->WeiboID word1 word2 word3
//	public static String split(String line){
//		String arr[]=line.split("\\s+");
//		String WeiboID=arr[0];//把weiboID提取出来
//		String arrmessage[]=new String[arr.length-1];
//		for(int i=1;i<arr.length;i++){
//			arrmessage[i-1]=arr[i];
//		}
//		String message=org.apache.commons.lang.StringUtils.join(arrmessage);//message为 WeiboID  message 中的message
//		StringBuffer sbout = new StringBuffer();
//		String result1 = transJe(message, "gb2312", "utf-8");
//		return WeiboID+" "+result1;
//		
//	}
//	public static void main(String[] args) {
//		Writer write=null;
//		try {
//			BufferedReader br = new BufferedReader(
//					new InputStreamReader(new FileInputStream(new File("/home/winstercheng/weibodata/test"))));
//			//写一个方法，实现输入一行输出一行
//			
//			StringBuffer sbin = new StringBuffer();
//			String line = null;
//			StringBuffer result=new StringBuffer();
//			while ((line = br.readLine()) != null) {
//				String r=split(line);
//				System.out.println("r is"+line);
//				result.append(r+"\n");
//				System.out.println(result);
//			}
//			write = new OutputStreamWriter(
//					new FileOutputStream(new File("/home/winstercheng/weibodata/test_after")));
//			write.append(result);
//			write.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//		}
//	}
//}
