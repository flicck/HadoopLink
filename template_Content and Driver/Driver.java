package cn.driver;


import org.apache.hadoop.util.ProgramDriver;

import cn.runner.Runner;

//通过反射调用Runner类的main方法
public class Driver {
	public static void main(String[] args) throws Throwable {
		int exitCode=-1;
		ProgramDriver driver=new ProgramDriver();
		//默认短命令为HadoopLink
		try {
			driver.addClass("HadoopLink", Runner.class, "HadoopLink");
			exitCode=driver.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(exitCode);
	}
}
