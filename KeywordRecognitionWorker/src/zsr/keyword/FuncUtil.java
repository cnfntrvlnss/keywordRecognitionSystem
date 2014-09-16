package zsr.keyword;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

public class FuncUtil {

	public static <T> T[] concate(T[] a, T[] b) {
		T[] c = Arrays.copyOf(a, a.length+b.length);
		System.arraycopy(b, 0, c, a.length, b.length);
		return c;
	}
	public static <T> T[] concateAll(T[] a, T[]...ts ){
		int len = a.length;
		for (T[] b : ts) {
			len += b.length;
		}
		T[] c = Arrays.copyOf(a,  a.length);
		len = a.length;
		for(T[]b: ts) {
			System.arraycopy(b, 0, c, len, b.length);
			len += b.length;
		}
		return c;
	}
	public static byte[] concat(byte[] res, byte[] tmpArr) {
		// TODO Auto-generated method stub
		byte[] c = Arrays.copyOf(res, res.length+tmpArr.length);
		System.arraycopy(tmpArr, 0, c, res.length, tmpArr.length);
		return c;
	}

	public static byte[] concateAll(byte[] a, byte[] ...bs ) {
		int len = a.length;
		for(byte[] b : bs){
			len += b.length;
		}
		byte[] c = Arrays.copyOf(a, a.length);
		len = a.length;
		for(byte[] b: bs) {
			System.arraycopy(b, 0, c, len, b.length);
			len += b.length;
		}
		return c;
	}
	
	/**
	 * 要确保写文件时，是操作系统级互斥的。
	 * 确保不抛出异常，但运行失败。
	 * @param file
	 */
	public static void writeIdxFile(String file, byte[] data) {
		File f = new File(file);
		if(data == null || f.exists()) {
			//log: warning, while the exact file exists, receive a creat-file
			//command.
			return;
		}
		File tF = new File(file +"."+ Thread.currentThread().getId() +".tmp");
		try{
			OutputStream out = new FileOutputStream(tF);
			out.write(data);
			out.close();
			tF.renameTo(f);
		}
		catch(FileNotFoundException e) {
			e.printStackTrace();
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		finally{
			if(tF.exists()){
				tF.delete();
			}
		}
	}
	/**
	 * 
	 * @param file
	 * @return
	 */
	public static byte[] readIdxFile(String file) {
		File f = new File(file);
		if(!f.exists()) {
			//log: warning, while the exact file doesn't exist, receive
			//a read command.
			return null;
		}
		byte[] res = null;
		InputStream in = null;
		try{
			in = new FileInputStream(f);
			byte[] tmpArr = new byte[1000];
			int readNum = in.read(tmpArr);
			while(readNum == 1000){
				if(res == null){
					res = tmpArr;
				}
				else {
					FuncUtil.concat(res, tmpArr);
				}
				readNum = in.read(tmpArr);
			}
			if(readNum > 0) {
				byte[] tail = Arrays.copyOf(tmpArr, readNum);
				FuncUtil.concat(res, tail);
			}
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		finally {
			try{
				if (in != null)
					in.close();		
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		return res;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	}

}
