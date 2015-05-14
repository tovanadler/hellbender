package org.broadinstitute.hellbender;

import org.testng.IMethodInstance;
import org.testng.ITestContext;

import java.util.ArrayList;
import java.util.List;

public class ParallelSplittingListener implements org.testng.IMethodInterceptor {
    private final int totalNodes;
    private final int nodeNumber;

    private static int count = 0;

    public ParallelSplittingListener(){
        String totalString = System.getenv("CIRCLE_NODE_TOTAL");
        String indexString = System.getenv("CIRCLE_NODE_INDEX");
        if ( totalString != null && indexString != null) {
            this.totalNodes = Integer.valueOf(totalString);
            this.nodeNumber = Integer.valueOf(indexString);
        } else {
            this.totalNodes = -1;
            this.nodeNumber = 1;
        }
    }

    @Override
    public List<IMethodInstance> intercept(List<IMethodInstance> methods, ITestContext context) {
        count++;
        if (totalNodes > 1 && nodeNumber > -1 ) {
            List<IMethodInstance> result = new ArrayList<>();
            for (IMethodInstance m : methods) {
                int shard = m.getMethod().getMethodName().hashCode() % totalNodes;
              //  System.out.println(shard);
                if (shard == nodeNumber) {
                    result.add(m);
                }
            }
            return result;
        } else {
            return methods;
        }
    }
}
