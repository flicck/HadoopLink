/**
 * @author   wanghan
*/

package cn.annotation;


import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
//必须
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented //javadoc
public @interface MyMapper {
	String value() default "";
	
}

