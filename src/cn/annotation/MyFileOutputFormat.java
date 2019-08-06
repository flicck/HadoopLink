/**
 * @author   wanghan
*/

package cn.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
//可选
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented //javadoc
public @interface MyFileOutputFormat {
	String value() default "org.apache.hadoop.mapreduce.lib.output.TextOutputFormat";
}
