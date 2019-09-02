# HadoopLink  
  HadoopLink is a framework that is further encapsulated based on Hadoop and BaseMR. This framework eliminates the tedious duplication of code that generates Job, JobControl, and ContrlledJob. At the same time, you can easily set the parallelism and dependencies between MapReduce tasks through annotations.  HadoopLink instructions for use can be viewed or downloaded online at http://www.techvision.top/  
	欢迎大家关注和使用HadoopLink，HadoopLink是基于Hadoop及BaseMR进一步封装而来的框架。该框架省去了您生成Job，JobControl，ContrlledJob的繁杂重复代码。同时您可以非常便捷的通过注释设置MapReduce任务之间的依赖关系。HadoopLink支持多个任务链并行运行，不同任务链的可以进行定时执行设置。
使用HadoopLink，您只需要编写多个MapReduce类和一个简单的Driver类就能完成任务链的依赖和并行关系。
  HadoopLink的使用说明文档请在http://www.techvision.top/ 在线查看或下载
  
java jdk>=1.8  
## HadoopLink 1.1.1
#### MAVEN:  
`<dependency>`  
  `  <groupId>io.github.hanwang1995</groupId>`  
  `  <artifactId>hadoopLink</artifactId>`  
  `  <version>1.1.1<version>`  
`</dependency>`  
#### 1.1.1版本更新信息：
##### 1.修复bug： 
　　+修复可选配置项未配置导致的空指针异常  	
## HadoopLink 1.1.0 
#### 1.1.0版本更新信息：  
##### 1.增强程序的灵活性：  
  只需要简单的实现一个EditConfigJob接口，您便可以  
　　+对任意一个工作链的任意一个mr类的Configuration单独进行设置  
　　+对任意一个工作链的任意一个mr类的Job单独进行设置  
　　+对任意一个工作链的线程完成任务前的最后阶段执行其他的操作  
　　>>注意：您对Configuration和Job的单独设置如果和默认设置有冲突，将以您单独设置的为准（虽然提供了这样的自由度，但强烈建议您不要在这自定义工作链内每个mr类的输入输出路径，程序会根据配置文件自动帮您完成，如您自己定义非常容易造成混乱）  
##### 2.优化了输出结果，使得每一个任务链的输出结果都单独在一个[]中，避免同一时间完成多个任务导致不同任务日志输出的counters混杂  
##### 3.增加了泛型中嵌套泛型的支持如Mapper<LongWritable,Text,AvroKey<GenericRecord>, NullWritable>  
##### 4.加快了执行速度
