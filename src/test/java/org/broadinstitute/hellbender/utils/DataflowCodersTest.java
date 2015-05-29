package org.broadinstitute.hellbender.utils;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.Lists;
import org.broadinstitute.hellbender.tools.dataflow.transforms.InsertSizeMetricsDataflowTransform;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.List;

/**
 * Created by louisb on 5/27/15.
 */
public class DataflowCodersTest {

    @Test
    public void testEncoding(){
        Pipeline p = TestPipeline.create();
        p.getCoderRegistry().registerCoder(A.class, SerializableCoder.of(A.class));
        p.getCoderRegistry().registerCoder(B.class, SerializableCoder.of(B.class));

        List<B<Integer,A<String>>>  Bs= Lists.newArrayList( new B<>(1, new A<>("Hi")), new B<>(2,new A<>("Bye")));
        PCollection<B<Integer,A<String>>> input = p.apply(Create.of(Bs));
        PCollection<A<B<Integer,A<String>>>> wrapped = input.apply(ParDo.of(new Wrap<>()));
        PCollection<String> strings = wrapped.apply(ParDo.of( new PrintLn<>()));
        strings.apply(ParDo.of(new PrintLn<>()));

        PipelineResult result = p.run();

    }

    public static class A<T> implements Serializable{
        public final T value;

        public A(T value) {
            this.value = value;
        }

        @Override
        public String toString(){
            return "A(" + this.value +")";
        }
    }


    public static class PrintLn<T> extends DoFn<T,String>{

        @Override
        public void processElement(ProcessContext c) throws Exception {
            String str = c.element().toString();
            System.out.println(str);
            c.output(str);
        }
    }

    public static class Wrap<T> extends DoFn<T,A<T>>{

        @Override
        public void processElement(ProcessContext c) throws Exception {

            c.output(new A<>(c.element()));
        }
    }

    public static class B<T,K extends A> extends A<T>{
        public final K secondValue;

        public B(T value, K secondValue) {
            super(value);
            this.secondValue = secondValue;
        }

        @Override
        public String toString(){
            return "B(" + value + ","+secondValue +")";
        }
    }

}
