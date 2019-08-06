/**
 * @author   wanghan
*/
//这个类只要在项目下就可以了，Runner会在整个项目下递归找这个类
package cn.driver;

public class Content {
	//-D参数常量设置
	//任务id,-D参数传入,默认会加到输出文件夹的后面,如果没有设置就不会加
	public static final String TASK_ID="task_id";
	//任务的输入地址,-D参数传入,几个任务链就定义几个输入地址，值的格式必须是task_input_加上数字，这个数字对应@MyMapReduce(2_1)中的第一个数字
	public static final String TASK_INPUT_1="task_input_1";
	public static final String TASK_INPUT_2="task_input_2";
	public static final String TASK_INPUT_3="task_input_3";
	//任务定时设置,时间由-D传入，如果没有传入就默认马上开始,传入参数为秒的值
	public static final String TASK_TIME_1="task_time_1";
	public static final String TASK_TIME_2="task_time_2";
	public static final String TASK_TIME_3="task_time_3";
	
	//非-D参数常量设置
	//windows
	public static final String TASK_WORK_BASE_PATH="D:/MapReduceTest/";
	//linux
//	public static final String TASK_WORK_BASE_PATH="/hadoop/output/";
	//设置最大并行工作链的数量
	public static final Integer MAX_TASK_LINK=20;
	//配置mapreduce类的扫描路径	
	public static final String SCANPACKAGE_MAPREDUCE="cn/mr";
	//配置自定义FileInputFormat的扫描路径 
	public static final String SCANPACKAGE_FILEINPUTFORMAT="cn/myInputFormat";
	//配置自定义FileOutputFormat的扫描路径 
	public static final String SCANPACKAGE_FILEOUTPUTFORMAT="cn/myOutputFormat";
	//配置是否自动删除原输出目录(不配置的话默认为false，不会自动删除)
	public static final Boolean AUTODELETE_OUTPATH=true;
}
