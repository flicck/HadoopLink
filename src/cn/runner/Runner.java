/**
 * @author   wanghan
*/

package cn.runner;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.module.SimpleAbstractTypeResolver;


import cn.annotation.MyCombiner;
import cn.annotation.MyFileInputFormat;
import cn.annotation.MyFileOutputFormat;
import cn.annotation.MyMapReduce;
import cn.annotation.MyMapper;
import cn.annotation.MyPartitioner;
import cn.annotation.MyReducer;
import cn.annotation.NumOfReducer;

import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

@SuppressWarnings({ "unused","unchecked", "rawtypes" })
public class Runner extends Configured implements Tool {
	//定义content类的静态变量
	private static Class ContentClass=null;
	private static int MaxTaskLink=0;
	private static String ScanPackage_MapReduce=null;
	private static String ScanPackage_FileInputFormat=null;
	private static String ScanPackage_FileOutputFormat=null;
	private static String TaskWorkBasePath=null;
	private static String ScanPackage_EditConfigJob=null;
	private static Boolean AutoDeleteOutPath=false;
	//获取实际要求运行的工作链编号
	private ArrayList<Integer> taskListNum=null;
	//任务定时参数map,key为任务编号,value为任务等待的毫秒值
	private HashMap<Integer,Long> fixedTime=new HashMap<>();
	//保存getRootPathStatic返回的rootpath以供其他类使用
	private static String rootPathStatic=null;
	private EditConfigJob editConfigJob=null;
	//类被加载的时候就获得Content.class到contentClass中
	static{
		try {
			getContentClass();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private static void getContentClass() throws ClassNotFoundException{
		Runner runner=new Runner();
		//如果是windows环境
		if("\\".equals(File.separator)){
			getRootPathStatic(runner);
			String path = runner.getClass().getClassLoader().getResource("//").getPath();
			File dic = new File(path);
			File[] files = dic.listFiles();
			findFile(files);
		}else if("/".equals(File.separator)){
			getRootPathStatic(runner);
			String path=rootPathStatic;
			File dic=new File(path);
			File[] files = dic.listFiles();
			findFile(files);
		}
	}
	private static void findFile(File[] files) throws ClassNotFoundException {
		for(File f:files){
			if(f.isDirectory()){
				File[] filez=f.listFiles();
				findFile(filez);
			}else{
				if(f.getName().equals("Content.class")){
					//如果是windows环境
					if("\\".equals(File.separator)){
						String contentClassPath=f.getAbsoluteFile().toString().split("classes")[1].split("\\.")[0].replace("\\", ".").substring(1
								,f.getAbsoluteFile().toString().split("classes")[1].split("\\.")[0].replace("\\", ".").length());
						ContentClass=Class.forName(contentClassPath);
					//如果是linux环境
					}else if("/".equals(File.separator)){
						String contentClassPath=f.getAbsoluteFile().toString().split("(hadoop-unjar)[0-9]+")[1].split("\\.")[0].replace("/", ".").substring(1
								,f.getAbsoluteFile().toString().split("(hadoop-unjar)[0-9]+")[1].split("\\.")[0].replace("/", ".").length());
						ContentClass=Class.forName(contentClassPath);
					}
					try {
						Field[] fields = ContentClass.getFields();
						HashSet<String> h1=new HashSet<>();
						for(Field field:fields){
							 String fieldstr = field.toString().split("\\.")[field.toString().split("\\.").length-1];
							 h1.add(fieldstr);
						}
						//一定要配置的
						MaxTaskLink=(int) ContentClass.getField("MAX_TASK_LINK").get(new Object());
						ScanPackage_MapReduce=(String) ContentClass.getField("SCANPACKAGE_MAPREDUCE").get(new Object());
						TaskWorkBasePath=(String) ContentClass.getField("TASK_WORK_BASE_PATH").get(new Object());
						AutoDeleteOutPath=(Boolean) ContentClass.getField("AUTODELETE_OUTPATH").get(new Object());
						//可以选择配置的
						if(h1.contains("SCANPACKAGE_FILEINPUTFORMAT")){
							ScanPackage_FileInputFormat=(String) ContentClass.getField("SCANPACKAGE_FILEINPUTFORMAT").get(new Object());
						}
						if(h1.contains("SCANPACKAGE_FILEOUTPUTFORMAT")){
							ScanPackage_FileOutputFormat=(String) ContentClass.getField("SCANPACKAGE_FILEOUTPUTFORMAT").get(new Object());
						}
						if(h1.contains("SCANPACKAGE_EDITCONFIGJOB")){
							ScanPackage_EditConfigJob=(String) ContentClass.getField("SCANPACKAGE_EDITCONFIGJOB").get(new Object());
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
		
	}
	//运行类
	public static void main(String[] args) throws ClassNotFoundException {
		try {
			ToolRunner.run(new Runner(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	//run类
	@Override
	public int run(String[] args) throws Exception {
		/**edit hadooplink1.1->step 1:获得通用的configuration
		 * edit by wanghan 2019.9.1
		 */
		Configuration conf = this.getConf();
		/**edit hadooplink1.1->step 2:获得用户自定义的EditConfigJob实现类
		 * edit by wanghan 2019.9.1
		 */	
		if(ScanPackage_EditConfigJob!=null){
			editConfigJob=this.getEditConfigJob();
		}
		/**edit hadooplink1.1->step 3:获得所有的任务链中的所有mr类
		 * edit by wanghan 2019.9.1
		 */		
		HashMap[] grandList = this.getTaskMapList();
		/**edit hadooplink1.1->step 4:获得所有的controlledJob
		 * edit by wanghan 2019.9.1
		 */	
		ArrayList<ControlledJob> controlledJobs = this.getControlledJobs(grandList, conf);	
		/**edit hadooplink1.1->step 5:建一个jobContrilList池备用，size为MaxTaskLink加1
		 * edit by wanghan 2019.9.1
		 */	
		ArrayList<JobControl> jobControlList=new ArrayList<>();
		for(int i=1;i<=MaxTaskLink+1;i++){
			jobControlList.add(new JobControl("工作链"+i));
		}
		/**edit hadooplink1.1->step 6:将controlledJobs中的controlledJob塞到JobControlList中
		 * edit by wanghan 2019.9.1
		 */
		int count=1;
		for(int i=0;i<controlledJobs.size();i++){
			if(null!=controlledJobs.get(i)){
				jobControlList.get(count).addJob(controlledJobs.get(i));
			}
			if(i+1<=controlledJobs.size()-1){
				if(null==controlledJobs.get(i) && null!=controlledJobs.get(i+1)){
					//代表前面有一个工作链了，需要一个新的JobControl
					count++;
				}
			}
		}
		/**edit hadooplink1.1->step 6:根据taskListNum的size往JobRunnerUtil.run方法里面传JobControl,同时从taskListNum里面取出对应的工作链序号
		 * edit by wanghan 2019.9.1
		 */
		Map<Object, Integer> exitFlagMap = new Hashtable<>();
		for(int i=1;i<=count;i++){
			JobControl jobcol=jobControlList.get(i);
			int k=taskListNum.get(i-1);
			//看一看当前任务是否设置了定时任务
			Long millisecond=fixedTime.get(k);
			if(0L!=millisecond){
				//说明设置了定时任务,传入时间格式化方法,格式化一下时间
				String formatTime=parseTime(millisecond);
				new Thread(new Runnable(){
					@Override
					public void run() {
						try {
							System.out.println("HadoopLink:task link "+k+" is timed");
							System.out.println("HadoopLink:task link "+k+" will run after"+formatTime);
							Thread.sleep(millisecond);
							System.out.println("HadoopLink:task link "+k+" start run");
							Object o=JobRunnerUtil.run(jobcol,k);
							/**edit hadooplink1.1->自定义对任意一个工作链的线程完成任务前的最后阶段执行其他的操作
							 * edit by wanghan 2019.9.1
							 */
							if(editConfigJob!=null){
							editConfigJob.editThread(conf, k);
							}
							exitFlagMap.put(o,1);
							
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}).start();
			}else{
				new Thread(new Runnable(){
					@Override
					public void run() {
						try {
							System.out.println("HadoopLink:task link "+k+" is not timed");
							System.out.println("HadoopLink:task link "+k+" start run");
							Object o=JobRunnerUtil.run(jobcol,k);
							/**edit hadooplink1.1->自定义对任意一个工作链的线程完成任务前的最后阶段执行其他的操作
							 * edit by wanghan 2019.9.1
							 */
							if(editConfigJob!=null){
								editConfigJob.editThread(conf, k);
							}
							exitFlagMap.put(o,1);
							
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}).start();
			}
			
		}
		//判断这个并发容器中收到的结果数量是不是工作链的数量
		while(exitFlagMap.size()!=count){
			Thread.sleep(1000);
		}
		System.out.println("HadoopLink:all links finished,now quit");
		return 0;
		
	}

	private String parseTime(Long mss) {
		//计算用了多少天
		Long days=mss/(1000*60*60*24);
		//计算用了多少个小时
		Long hours = (mss%(1000*60*60*24))/(1000*60*60);
		//计算用了多少分钟
		Long minutes = (mss%(1000*60*60))/(1000*60);
		//计算用了多少秒钟
		Long seconds=(mss%(1000*60))/1000;
		//开始拼凑时间
		StringBuilder sb=new StringBuilder();
		//判断
		if(days!=0){
			sb.append(days).append("days");
		}
		if(hours!=0){
			sb.append(hours).append("hours");
		}
		if(minutes!=0){
			sb.append(minutes).append("minutes");
		}
		if(seconds!=0){
			sb.append(seconds).append("seconds");
		}
		return sb.toString();
	}
		
	
	// getjobs就是要简单的获取一下所有的job
	private ArrayList<ControlledJob> getControlledJobs(HashMap[] grandList, Configuration conf) {
		// 从conf的args里面获得并行任务列表--有几个-d输入，就有几个并行任务
		taskListNum = getTaskListNum(conf);
		if(taskListNum.size()==0){
			try {
				throw new PleaseInputYourCommandException("You have not entered an input path, or the input path is malformed");
			} catch (PleaseInputYourCommandException e) {
				e.printStackTrace();
				System.out.println("HadoopLink:quit");
				System.exit(0);
			}
		}
		
		// 初始化taskLink小数组，由context设置长度
		int maxTaskLink = MaxTaskLink + 1;
		HashMap[] taskList = new HashMap[maxTaskLink];
		// 从grandList中筛选这些任务链出来，放到taskList
		for (Integer j : taskListNum) {
			taskList[j] = grandList[j];
		}
		ArrayList<ControlledJob> ControlledJobArray = new ArrayList<>();
		
		//外层循环，是所有的mr类
		for (int j = 1; j < taskList.length; j++) {
			HashMap tmpMap = taskList[j];
			//里层循环是每一个工作链的mr类
			if (null != tmpMap) {
				TreeMap tm = new TreeMap(tmpMap);
				Iterator it = tm.entrySet().iterator();

				Map.Entry<Integer, ArrayList> entry = null;
						
				// 执行顺序标识
				int i = 0;
				// 保存上一个运行任务的输出路径和job
				Path outTransIn = null;
				ControlledJob dependedJob = null;
				while (it.hasNext()) {

					Job tmpJob = null;
					entry = (Entry) it.next();
					// 先拿顺序第一的
					
					ArrayList classesList = entry.getValue();
					Class topClass = (Class) classesList.get(0);
					String str2 = topClass.getName();
					// 从Conf复制jobConf给项目使用
					Configuration jobConf = new Configuration();
					for (java.util.Map.Entry<String, String> srcConf : conf) {
						jobConf.set(srcConf.getKey(), srcConf.getValue());
					}
					/**edit hadoolink1.1-》增加自定义configuration方法
					 * 
					 */	
					if(editConfigJob!=null){
						editConfigJob.editConfig(jobConf, j, i+1);
					}
					// 拿到了顶级类
					try {
						tmpJob = Job.getInstance(jobConf, str2);
					} catch (IOException e) {
						e.printStackTrace();
					}
					tmpJob.setJarByClass(topClass);
					// 拿一下MyReducer的值，设置一下reducer的数量
					int myReducerNum = getReducerNum(topClass);
					tmpJob.setNumReduceTasks(myReducerNum);
					// 好了i=0的工作已结束，处理剩下的内部类
					for (int j1 = 1; j1 < classesList.size(); j1++) {
						Class innerClass = (Class) classesList.get(j1);
						// 判断这个类是什么类
						String str = parseInnerClass(innerClass);
						// 啥都没有的话报错
						if (null == str) {
							try {
								throw new DontFindAnotationException("don't find "+topClass.getName()+"'s innerClass Annotation");
							} catch (DontFindAnotationException e) {
								e.printStackTrace();
								System.out.println("HadoopLink:quit");
								System.exit(0);
							}
						}
						if (str.equals("Mapper")) {
							tmpJob.setMapperClass(innerClass);
							// 然后获取一下泛型列表，设置一下输入输出类型
							Class[] genericClasses = getGenericClasses(innerClass);
							tmpJob.setMapOutputKeyClass(genericClasses[0]);
							tmpJob.setMapOutputValueClass(genericClasses[1]);
						} else if (str.equals("Reducer")) {
							tmpJob.setReducerClass(innerClass);
							Class[] genericClasses = getGenericClasses(innerClass);
							tmpJob.setOutputKeyClass(genericClasses[0]);
							tmpJob.setOutputValueClass(genericClasses[1]);
						} else if (str.equals("MyPartitioner")) {
							tmpJob.setPartitionerClass(innerClass);
						} else if (str.equals("MyCombiner")) {
							tmpJob.setCombinerClass(innerClass);
						}

					}
					// 最后设置一下输入与输出路径和输入输出格式，需要判断一下是不是第一个执行的类
					// 如果是第一个执行的，输入路径需要-D参数传入
					// 如果不是第一个执行的，输入路径采用前一个的输出路径

					// 设置输入输出格式
					// 输入
					String inputFormatClassPath = getInputFormatClassPath(topClass);
					// 没有找到path的话，抛异常
					if (null == inputFormatClassPath) {
						try {
							throw new DontFindFormatClassException("don't find your InputFormatClass");
						} catch (DontFindFormatClassException e) {
							e.printStackTrace();
							System.out.println("HadoopLink:quit");
							System.exit(0);
						}
					}
					if (!inputFormatClassPath.equals("org.apache.hadoop.mapreduce.lib.input.TextInputFormat")) {
						//如果是windows环境
						if("\\".equals(File.separator)){
							String inputFormatClass = inputFormatClassPath.split("classes")[1].replace("\\", ".")
									.substring(1, inputFormatClassPath.split("classes")[1].replace("\\", ".").length() - 6);
							try {
								tmpJob.setInputFormatClass((Class<? extends InputFormat>) Class.forName(inputFormatClass));
							} catch (Exception e1) {
								e1.printStackTrace();
							}
						}else if("/".equals(File.separator)){
							String inputFormatClass = inputFormatClassPath.split("(hadoop-unjar)[0-9]+")[1].replace("/", ".")
									.substring(1,inputFormatClassPath.split("(hadoop-unjar)[0-9]+")[1].replace("/", ".").length()-6);
							try {
								tmpJob.setInputFormatClass((Class<? extends InputFormat>) Class.forName(inputFormatClass));
							} catch (Exception e1) {
								e1.printStackTrace();
							}
						}
					} else {
						try {
							tmpJob.setInputFormatClass(
									(Class<? extends InputFormat>) Class.forName(inputFormatClassPath));
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					// 输出
					String outputFormatClassPath = getOutputFormatClassPath(topClass);
					// 没有找到path的话，抛异常
					if (null == outputFormatClassPath) {
						try {
							throw new DontFindFormatClassException("don't find your InputFormatClass");
						} catch (DontFindFormatClassException e) {
							e.printStackTrace();
							System.out.println("HadoopLink:quit");
							System.exit(0);
						}
					}
					if (!outputFormatClassPath.equals("org.apache.hadoop.mapreduce.lib.output.TextOutputFormat")) {
						//如果是window环境
						if("\\".equals(File.separator)){
							String outputFormatClass = outputFormatClassPath.split("classes")[1].replace("\\", ".")
									.substring(1,
											outputFormatClassPath.split("classes")[1].replace("\\", ".").length() - 6);
							try {
								tmpJob.setOutputFormatClass(
										(Class<? extends OutputFormat>) Class.forName(outputFormatClass));
							} catch (Exception e1) {
								e1.printStackTrace();
							}
						}else if("/".equals(File.separator)){
							String outputFormatClass = outputFormatClassPath.split("(hadoop-unjar)[0-9]+")[1].replace("/", ".")
									.substring(1,
											outputFormatClassPath.split("(hadoop-unjar)[0-9]+")[1].replace("/", ".").length() - 6);
							try {
								tmpJob.setOutputFormatClass(
										(Class<? extends OutputFormat>) Class.forName(outputFormatClass));
							} catch (Exception e1) {
								e1.printStackTrace();
							}
						}
					} else {
						try {
							tmpJob.setOutputFormatClass(
									(Class<? extends OutputFormat>) Class.forName(outputFormatClassPath));
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					// 设置路径
					if (i == 0) {
						// 说明是第一个执行，-D设置一下输入路径
						String taskInputCommand = "task_input_" + j;
						try {
							FileInputFormat.addInputPath(tmpJob, new Path(jobConf.get(taskInputCommand)));
						} catch (Exception e) {
							e.printStackTrace();
						}
						//顺便把任务定时信息放到map里去
						String taskTimeCommand="task_time_"+j;
						Long millisecond=0L;
						if(null!=jobConf.get(taskTimeCommand)){
							try {
								millisecond=Long.parseLong(jobConf.get(taskTimeCommand))*1000;
							} catch (NumberFormatException e1) {
								e1.printStackTrace();
							}
						}
						fixedTime.put(j, millisecond);
					} else {
						// 说明不是第一个执行的，需要设置输入目录为上一个任务的输出目录
						try {
							FileInputFormat.addInputPath(tmpJob, outTransIn);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					// 再设置一下输出路径
					// 需要先获得一下任务名，任务名在mapreduce的annotation的值的前一部分，通过反射获取
					String jobName = getJobName(topClass);
					Path outputPath=null;
					if(null!=jobConf.get("task_id")){
						outputPath = new Path(TaskWorkBasePath +j+File.separator+jobName+"_"+jobConf.get("task_id"));
					}else{
						outputPath = new Path(TaskWorkBasePath +j+File.separator+jobName);
					}
					
					// 判断一下是否目录已经存在，如果删除标识为true就删掉
					try {
						FileSystem fs = FileSystem.get(jobConf);
						if (fs.exists(outputPath)) {
							if(AutoDeleteOutPath){
								fs.delete(outputPath, true);
								System.out.println("HadoopLink:task link "+j+" "+jobName+" original output directory has been deleted");
							}else{
								//抛无法删除异常
								try {
									throw new YourOutputPathIsAlreadyExisted("taskLink"+j+"'s "+jobName+"'s outputPath is already Existed，and you"
											+ " set delete tag false or you do not set delete tag");
								} catch (YourOutputPathIsAlreadyExisted e) {
									e.printStackTrace();
									System.out.println("HadoopLink:quit");
									System.exit(0);
								}
							}
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					FileOutputFormat.setOutputPath(tmpJob, outputPath);
					/**edit hadooplink1.1->增加对job的修改方法
					 * edit wanghan
					 */
					if(editConfigJob!=null){
						editConfigJob.editJob(tmpJob, j, i+1);
					}
					
					
					// 全部设置完毕，创建一个ControlledJob对象吧
					ControlledJob controlledJob = null;
					try {
						controlledJob = new ControlledJob(jobConf);
					} catch (IOException e) {
						e.printStackTrace();
					}
					controlledJob.setJob(tmpJob);
					// 判断一下当前controlledJob是否需要设置依赖
					if (i == 0) {
						// 不需要设置依赖
						// 保存一下这个job和输出路径,供后面的job使用
						outTransIn = outputPath;
						dependedJob = controlledJob;
						// 将这个Controlledjob保存到一个list集合里面去
						ControlledJobArray.add(controlledJob);
					} else if (i > 0) {
						// 需要设置依赖
						controlledJob.addDependingJob(dependedJob);
						// 同样保存job和输出路径
						outTransIn = outputPath;
						dependedJob = controlledJob;
						// 将这个Controlledjob保存到一个list集合里面去
						ControlledJobArray.add(controlledJob);
					}

					i++;

				}
				//此时，一个工作链的job已经都设置到arraylist里面去了，往arraylist里放一个null值占位置，用于标识不同的工作链
				ControlledJobArray.add(null);
			}
			
		}
		return ControlledJobArray;
	}
	//该方法返回自定义OutputFormat类的路径
	private String getOutputFormatClassPath(Class topClass) {
		if (topClass.isAnnotationPresent(MyFileOutputFormat.class)) {
			MyFileOutputFormat myFileOutputFormat = (MyFileOutputFormat) topClass
					.getAnnotation(MyFileOutputFormat.class);
			String outputFormatClassPath = myFileOutputFormat.value();
			if (!outputFormatClassPath.equals("org.apache.hadoop.mapreduce.lib.output.TextOutputFormat")) {
				outputFormatClassPath = findAbsoluteClassPath(outputFormatClassPath,
						ScanPackage_FileOutputFormat);
			}
			return outputFormatClassPath;
		}
		// 没有写的话，返回默认值
		return "org.apache.hadoop.mapreduce.lib.output.TextOutputFormat";

	}
	//该方法返回自定义InputFormat类的路径
	private String getInputFormatClassPath(Class topClass) {
		if (topClass.isAnnotationPresent(MyFileInputFormat.class)) {
			MyFileInputFormat myFileInputFormat = (MyFileInputFormat) topClass.getAnnotation(MyFileInputFormat.class);
			String inputFormatClassPath = myFileInputFormat.value();
			if (!inputFormatClassPath.equals("org.apache.hadoop.mapreduce.lib.input.TextInputFormat")) {
				inputFormatClassPath = findAbsoluteClassPath(inputFormatClassPath, ScanPackage_FileInputFormat);
			}
			return inputFormatClassPath;
		}
		// 没有写的话，返回默认值
		return "org.apache.hadoop.mapreduce.lib.input.TextInputFormat";

	}
	//该方法通过传入的自定义format类的名字和扫描路径找到对应的类，返回绝对路径
	private String findAbsoluteClassPath(String FormatClassName, String scanPackagePath) {
		String rootPath=rootPathStatic;
		String path=rootPath+scanPackagePath;
		File dic=new File(path);
		File[] files=dic.listFiles();
		for(File subfile:files){
			String subfileName=subfile.getName();
			if(subfileName.substring(0,subfileName.length()-6).equals(FormatClassName)){
				return subfile.getAbsolutePath();
			}else{
				continue;
			}
		}
		return null;


	}

	// 通过-D参数知道哪些任务链要运行
	private ArrayList<Integer> getTaskListNum(Configuration conf) {

		
		Field[] fields = ContentClass.getFields();
		ArrayList<Integer> taskList = new ArrayList<Integer>();
		for (Field field : fields) {
			String str = field.getName();
			if (null != conf.get(str.toLowerCase()) && str.toLowerCase().matches("^(task_input_).*")) {
				int tmpInt = Integer.parseInt(str.split("_")[2]);
				taskList.add(tmpInt);
			}
		}
		return taskList;

	}
	
	//该方法返回MapReduce的顶级类
	private String getJobName(Class topClass) {
		if (topClass.isAnnotationPresent(MyMapReduce.class)) {
			MyMapReduce myMapReduce = (MyMapReduce) topClass.getAnnotation(MyMapReduce.class);
			String executeNum = myMapReduce.value();
			if (executeNum.split("_").length == 3) {
				return executeNum.split("_")[0];
				// 防止有些人在名称里面加_
			} else if (executeNum.split("_").length > 3) {
				String[] strs = executeNum.split("_");
				String tmpstr = null;
				for (int i = 0; i < strs.length - 2; i++) {
					tmpstr += strs[i];
				}
				return tmpstr;
			} else {
				// 如果填写不符合规范的话，默认为类名
				return topClass.getName();
			}
		}
		return null;

	}
	//该方法获得Mapper和Reducer的输出泛型
	private Class[] getGenericClasses(Class innerClass) {
		/**edit hadooplink1.1->新增对ParameterizedTypeImpl类的支持
		 * edit by wanghan 2019.9.1
		 */
		Type type = innerClass.getGenericSuperclass();
		ParameterizedType p = (ParameterizedType) type;
		Class keyClass=null;
		Class valueClass=null;
		if(p.getActualTypeArguments().length==4){
			 if (!(p.getActualTypeArguments()[2] instanceof ParameterizedTypeImpl)){ 
				 keyClass = (Class) p.getActualTypeArguments()[2];
			 }else{
				 ParameterizedTypeImpl pti=(ParameterizedTypeImpl) p.getActualTypeArguments()[2];
				 keyClass = pti.getRawType();
			 }
			 if (!(p.getActualTypeArguments()[3] instanceof ParameterizedTypeImpl)){ 
				 valueClass = (Class) p.getActualTypeArguments()[3];
			 }else{
				 ParameterizedTypeImpl pti1=(ParameterizedTypeImpl) p.getActualTypeArguments()[3];
				 valueClass = pti1.getRawType();
			 }
		}else if(p.getActualTypeArguments().length==2){
			if (!(p.getActualTypeArguments()[0] instanceof ParameterizedTypeImpl)){ 
				 keyClass = (Class) p.getActualTypeArguments()[0];
			 }else{
				 ParameterizedTypeImpl pti=(ParameterizedTypeImpl) p.getActualTypeArguments()[0];
				 keyClass = pti.getRawType();
			 }
			 if (!(p.getActualTypeArguments()[1] instanceof ParameterizedTypeImpl)){ 
				 valueClass = (Class) p.getActualTypeArguments()[1];
			 }else{
				 ParameterizedTypeImpl pti1=(ParameterizedTypeImpl) p.getActualTypeArguments()[1];
				 valueClass = pti1.getRawType();
			 }
		}
		Class[] classes = new Class[2];
		classes[0] = keyClass;
		classes[1] = valueClass;
		return classes;

	}
	//该方法判断内部类是哪种类
	private String parseInnerClass(Class innerClass) {
		if (innerClass.isAnnotationPresent(MyMapper.class)) {
			return "Mapper";
		} else if (innerClass.isAnnotationPresent(MyReducer.class)) {
			return "Reducer";
		} else if (innerClass.isAnnotationPresent(MyPartitioner.class)) {
			return "MyPartitioner";
		} else if (innerClass.isAnnotationPresent(MyCombiner.class)) {
			return "MyCombiner";
		}
		return null;

	}
	//该方法返回Reducer的数量
	private int getReducerNum(Class topClass) {
		if (topClass.isAnnotationPresent(NumOfReducer.class)) {
			NumOfReducer numOfReducer = (NumOfReducer) topClass.getAnnotation(NumOfReducer.class);
			String executeNum = numOfReducer.value();
			// 把值返回，如果没有设置值的话有默认值1
			return Integer.parseInt(executeNum);
		}
		// 没有写的话，默认返回1，代表一个reducer
		return 1;

	}
	//该方法返回一个EditConfigJob实例
	public EditConfigJob getEditConfigJob() throws ClassNotFoundException{
		String rootPath=rootPathStatic;
		String path = rootPath+ScanPackage_EditConfigJob;
		File dic = new File(path);
		File[] files = dic.listFiles();
		//指定的包中没有相关的类，返回null
		if(files.length==0){
			System.out.println("hhh shi wo la");
			return null;
		}
		//如果指定的包中不止一个类，抛异常退出
		if(files.length!=1){
			try {
				throw new YourEditConfigJobException("您SCANPACKAGE_EDITCONFIGJOB下的类多于1个");
			} catch (YourEditConfigJobException e) {
				e.printStackTrace();
				System.out.println("HadoopLink:quit");
				System.exit(0);
			}
		}
		File editConfigJobClassFile = files[0];
		Class editConfigJobClass=null;
		if ("\\".equals(File.separator)){
			String tmp = editConfigJobClassFile.getAbsolutePath().split("classes")[1];
			tmp = tmp.replace("\\", ".");
			tmp = tmp.substring(1, tmp.length() - 6);
			editConfigJobClass = Class.forName(tmp);
		}else if("/".equals(File.separator)){
			String tmp=editConfigJobClassFile.getAbsolutePath().split("(hadoop-unjar)[0-9]+")[1];
			tmp=tmp.replace("/", ".");
			tmp=tmp.substring(1,tmp.length()-6);
			editConfigJobClass=Class.forName(tmp);
		}
		EditConfigJob editConfigJob=null;
		try {
			editConfigJob=(EditConfigJob)editConfigJobClass.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}		
		return editConfigJob;
		
	}
	//该方法扫描所有MapReduce类及其内部类，将其最终加入到HashMap中
	public HashMap[] getTaskMapList() throws ClassNotFoundException {
		Runner test = new Runner();
		// URL url =
		// test.getClass().getClassLoader().getResource(Content.scanPackage);
		//给一个方法传入这个test对象，让它通过判断是windows还是linux返回根目录
		String rootPath=rootPathStatic;
		String path = rootPath+ScanPackage_MapReduce;
		
		File dic = new File(path);
		File[] files = dic.listFiles();
		Class topClass = null;
		Class innerClass = null;
		// 初始化大数组，由context设置长度
		int maxTaskLink = MaxTaskLink + 1;
		HashMap[] grandList = new HashMap[maxTaskLink];
		for (File file : files) {
			// 外层if获取顶级class
			if (!file.getName().matches(".+\\${1}.+")) {
				//如果是windows环境
				if ("\\".equals(File.separator)){
					String tmp = file.getAbsolutePath().split("classes")[1];
					tmp = tmp.replace("\\", ".");
					tmp = tmp.substring(1, tmp.length() - 6);
					topClass = Class.forName(tmp);
				//如果是linux环境
				}else if("/".equals(File.separator)){
					String tmp=file.getAbsolutePath().split("(hadoop-unjar)[0-9]+")[1];
					// /cn/runner/dad$fsdf.class
					tmp=tmp.replace("/", ".");
					tmp=tmp.substring(1,tmp.length()-6);
					topClass=Class.forName(tmp);
				}
				// 不管myMapReducer的注释是什么，都需要建一个arraylist包含这个mapreduce的所有顶级类和内部类
				ArrayList<Class> classesList = new ArrayList<Class>();
				classesList.add(topClass);
				// 在已获得的顶级类的基础上获得其内部类，并将其添加到classesList中去
				for (File subfile : files) {
					String subfileName = subfile.getName();
					if (subfileName.startsWith(file.getName().split("\\.")[0] + "$")
							&& subfileName.length() - 6 > file.getName().split("\\.")[0].length()) {
						String tmp2=null;
						//如果是windows环境
						if("\\".equals(File.separator)){
							tmp2 = subfile.getAbsolutePath().split("classes")[1];
							tmp2 = tmp2.replace("\\", ".");
							tmp2 = tmp2.substring(1, tmp2.length() - 6);
						//如果是linux环境
						}else if("/".equals(File.separator)){
							tmp2=subfile.getAbsolutePath().split("(hadoop-unjar)[0-9]+")[1];
							tmp2=tmp2.replace("/", ".");
							tmp2=tmp2.substring(1,tmp2.length()-6);
						}
						

						innerClass = Class.forName(tmp2);
						classesList.add(innerClass);
					}
				}
				// 判断这个顶级类的第一个数字是几，然后看granList里面是不是已经有对应的工作链了
				int parellelNum = parseParallelNum(topClass);
				if (parellelNum == 0) {
					try {
						throw new DontFindAnotationException("don't find "+topClass.getName()+"'s Annotation:MyMapReduce");
					} catch (DontFindAnotationException e) {
						e.printStackTrace();
						System.out.println("HadoopLink:quit");
						System.exit(0);
					}
				} else if (parellelNum > 0) {
					if (null != grandList[parellelNum]) {
						// 说明已经有工作链了，这时获得一下topclass的工作链顺序，将topclass的一家classes放到工作链的对应位置
						int executeNum = parseExecuteNum(topClass);
						if (executeNum == 0) {
							try {
								throw new DontFindAnotationException("don't find "+topClass.getName()+"'s Annotation:MyMapReduce");
							} catch (DontFindAnotationException e) {
								e.printStackTrace();
								System.out.println("HadoopLink:quit");
								System.exit(0);
							}
						} else if (executeNum > 0) {
							grandList[parellelNum].put(executeNum, classesList);
						}
					} else if (null == grandList[parellelNum]) {
						// 说明此时工作链还没有形成，需要新建一个工作链map
						HashMap<Integer, List> executeTree = new HashMap<>();
						// 将一家人存到这个map里
						int executeNum = parseExecuteNum(topClass);
						executeTree.put(executeNum, classesList);
						// 把map放到对应位置的grandList里面去
						grandList[parellelNum] = executeTree;
					}
				}
			}

		}
		return grandList;
	}
	
	//静态获得rootPath方法
	private static void getRootPathStatic(Runner test) {
		String rootPath=null;
		String currentPath=null;
		//windows环境下
		if ("\\".equals(File.separator)) {
			rootPath = test.getClass().getClassLoader().getResource("").getPath();
			rootPathStatic=rootPath;
		//linux hadoop -jar命令下
		}else if("/".equals(File.separator)){
			//hadoop -jar无法像windows一样通过加载器获得路径，故先拿到本类的绝对路径
			currentPath=test.getClass().getResource("").getPath();
		//	currentPath="/tmp/hadoop-unjar123123/cn/runner/";
			String[] strs=currentPath.split("/");
			rootPath="";
			for(String str:strs){
				if(!str.matches("^(hadoop-unjar).*")){
					str+="/";
					rootPath+=str;
				}else if(str.matches("^(hadoop-unjar).*")){
					str+="/";
					rootPath+=str;
					break;
				}
			}
			rootPathStatic=rootPath;			
		}else{			
		}	
	}
	
	// 获得平行任务的任务id---第一个值
	private static int parseParallelNum(Class topClass) {
		if (topClass.isAnnotationPresent(MyMapReduce.class)) {
			MyMapReduce myMapReduce = (MyMapReduce) topClass.getAnnotation(MyMapReduce.class);
			String executeNum = myMapReduce.value();
			if (executeNum.split("_").length == 3) {
				return Integer.parseInt(executeNum.split("_")[1]);
				// 防止有些人在名称里面加_
			} else if (executeNum.split("_").length > 3) {
				return Integer.parseInt(executeNum.split("_")[executeNum.split("_").length - 2]);
			} else {
				// 如果填写不符合规范的话，默认为第一个任务链
				return 1;
			}
		}
		// 没有写的话，返回0并抛异常
		return 0;
	}

	// 获取依赖任务的任务id---第二个值
	private static int parseExecuteNum(Class topClass) {
		if (topClass.isAnnotationPresent(MyMapReduce.class)) {
			MyMapReduce myMapReduce = (MyMapReduce) topClass.getAnnotation(MyMapReduce.class);
			String executeNum = myMapReduce.value();
			if (executeNum.split("_").length == 3) {
				return Integer.parseInt(executeNum.split("_")[2]);
				// 防止有些人在名称里面加_
			} else if (executeNum.split("_").length > 3) {
				return Integer.parseInt(executeNum.split("_")[executeNum.split("_").length - 1]);
			} else {
				// 如果填写不符合规范的话，默认为第一个执行的任务
				return 1;
			}

		}
		// 没有写的话，返回0并抛异常
		return 0;
	}

}
//没有找到注释异常类
class DontFindAnotationException extends Exception {
	public DontFindAnotationException(String message) {
		super(message);
	}
}
//没有找到自定义FormatClass异常类
class DontFindFormatClassException extends Exception {
	public DontFindFormatClassException(String message) {
		super(message);
	}
}
//没有输入命令异常类
class PleaseInputYourCommandException extends Exception{
	public PleaseInputYourCommandException(String message){
		super(message);
	}
}
//没有输入命令异常类
class YourEditConfigJobException extends Exception{
	public YourEditConfigJobException(String message){
		super(message);
	}
}
//删除标识为false异常
class YourOutputPathIsAlreadyExisted extends Exception{
	public YourOutputPathIsAlreadyExisted(String message){
		super(message);
	}
}


//支持各种对象的判断类
class Utils{
	public static Boolean isEmpty(Object obj){
		if(null==obj){
			return true;
		}else if(obj instanceof String){
			return "".equals(String.valueOf(obj).trim());
		}else if(obj instanceof Map<?,?>){
			return ((Map<?,?>)obj).isEmpty();
		}else if(obj instanceof Collection<?>){
			return ((Collection<?>)obj).isEmpty();
		}else if(obj.getClass().isArray()){	//判断是否为数组
			return Array.getLength(obj)==0;
		}
		return false;
	}
}
//创建线程池--任务工作链的运行工具类
class JobRunnerUtil{
	//创建一个监控线程池
	private static ExecutorService es=Executors.newCachedThreadPool();
	//创建一个ThreadLocal用于存放工作链序号
	private static ThreadLocal<String> threadLocal=new ThreadLocal<>();
	//JobRunnerResult是返回值
	private static class JobCallable implements Callable<JobRunnerResult>{
		//定义工作链对象
		private JobControl jobc;
		//定义工作链序号
		private int index;
		public JobCallable(JobControl jobc,int index){
			super();
			this.jobc=jobc;
			this.index=index;
		}
		@Override
		public JobRunnerResult call() throws Exception {
			//创建方法的返回值
			JobRunnerResult jrr=new JobRunnerResult();
			//创建任务执行的开始时间
			Long starttime=System.currentTimeMillis();
			//检测所有任务是否都执行完成
			while(!this.jobc.allFinished()){
				Thread.sleep(1000);
			}
			//获取所有的counter
			for(ControlledJob job:this.jobc.getSuccessfulJobList()){
				jrr.setCounterMap(job.getJobName(), job.getJob().getCounters());
			}
			//根据失败任务判断整个任务工作链是否成功
			jrr.setSuccess(jobc.getFailedJobList().size()==0);
			//创建结束时间
			long endtime=System.currentTimeMillis();
			//计算任务运行时间
			long runtime=endtime-starttime;
			//设置运行时间
			jrr.setRunTime(this.getLifeTime(runtime));
			//打印信息
			String finalresult="HadoopLink:[task Link "+index+(jrr.isSuccess()?" SUCCESS":" FAILD")+"\n"+"  take time : "+jrr.getRunTime()+"\n"+
					jrr.getCounterMap().toString().replace(",", "\n")+"]";
			System.out.println(finalresult);
			jobc.stop();
			return jrr;
		}
		//格式化时间方法
		private String getLifeTime(long mss) {
			//计算用了多少天
			Long days=mss/(1000*60*60*24);
			//计算用了多少个小时
			Long hours = (mss%(1000*60*60*24))/(1000*60*60);
			//计算用了多少分钟
			Long minutes = (mss%(1000*60*60))/(1000*60);
			//计算用了多少秒钟
			Long seconds=(mss%(1000*60))/1000;
			//开始拼凑时间
			StringBuilder sb=new StringBuilder();
			//判断
			if(days!=0){
				sb.append(days).append(" days");
			}
			if(hours!=0){
				sb.append(hours).append(" hours");
			}
			if(minutes!=0){
				sb.append(minutes).append(" minutes");
			}
			if(seconds!=0){
				sb.append(seconds).append(" seconds");
			}
			return sb.toString();
		}
		
		
	}
	//运行任务工作链,j是工作链序号
	public static JobRunnerResult run(JobControl jobc, int j)throws Exception{
		//启动任务工作链
		Thread t=new Thread(jobc);
		
		t.start();		
		
		//线程池的作用就在这里了---用于监控任务链是否完成，如果完成的话就返回结果
		JobRunnerResult.setIndex(j);
		JobCallable jobCallable = new JobCallable(jobc,j);
		
		Future<JobRunnerResult> f=es.submit(jobCallable);
		
		return f.get();
	}
}
//获得任务链中所有任务对应的counters类以及线程的
class JobRunnerResult{
	
	private static int index=0;
	//任务运行结果
	private boolean isSuccess;
	//任务运行时间
	private String runTime;
	private Map<String,Counters> counterMap=new HashMap<String,Counters>();
	//get set方法
	
	public boolean isSuccess(){
		return isSuccess;
	}
	public static int getIndex() {
		return index;
	}
	public static void setIndex(int index) {
		JobRunnerResult.index = index;
	}
	public void setSuccess(boolean isSuccess){
		this.isSuccess=isSuccess;
	}
	public String getRunTime(){
		return runTime;
	}
	public void setRunTime(String runTime){
		this.runTime=runTime;
	}
	public Map<String,Counters> getCounterMap(){
		return counterMap;
	}
	public void setCounterMap(String jobName,Counters counters){
		this.counterMap.put(jobName, counters);
	}
	//根据任务链名称，获取counters
	public Counters getCounters(String jobName){
		return counterMap.get(jobName);
	}
	//根据工作链控制的任务名称，获取counters
	public Counters getCounters(ControlledJob colJob){
		return counterMap.get(colJob.getJobName());
	}
	//获取指定的计数器
	public Counter getCounter(ControlledJob job,String gname,String cname){
		Counter counter=getCounters(job).findCounter(gname,cname);
		return counter;
	}
	//获取指定counter的值
	public long getCounterVal(ControlledJob job,String gname,String cname){
		//获取这个计数器
		Counter counter=getCounter(job,gname,cname);
		return Utils.isEmpty(counter)?0L:counter.getValue();
	}
}











