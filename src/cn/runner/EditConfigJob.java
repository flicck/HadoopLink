package cn.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
/**edit hadooplink1.1->EditConfigJob接口
 * @author wanghan 2019 9 1
 */
public interface EditConfigJob {
	/**edit hadooplink1.1->对任意一个工作链的任意一个mr类的Configuration单独设置
	 * edit wanghan 2019 9 1
	 * @param conf 				您需要设置的MR类所拥有的Configuration类
	 * @param parallelIndex		您需要设置的MR类的并行编号，如对应wordCount_1_2的1
	 * @param sequenceIndex		您需要设置的MR类的执行顺序编号，如对应wordCount_1_2的2
	 */
	public void editConfig(Configuration conf,int parallelIndex,int sequenceIndex);
	/**edit hadooplink1.1->对任意一个工作链的任意一个mr类的Job单独设置
	 * edit wanghan 2019 9 1
	 * @param job 				您需要设置的MR类所拥有的Job类
	 * @param parallelIndex		您需要设置的MR类的并行编号，如对应wordCount_1_2的1
	 * @param sequenceIndex		您需要设置的MR类的执行顺序编号，如对应wordCount_1_2的2
	 */
	public void editJob(Job job,int parallelIndex,int sequenceIndex);
	/**edit hadooplink1.1->对任意一个工作链的线程完成任务前的最后阶段执行其他的操作
	 * edit wanghan 2019 9 1
	 * @param conf 				通用的configuration
	 * @param parallelIndex		您需要设置的工作链的并行编号，如对应wordCount_1_1和wordCount_1_2的第一个数字1
	 */
	public void editThread(Configuration conf,int parallelIndex);
}
