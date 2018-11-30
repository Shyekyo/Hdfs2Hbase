package com.nokia.share.serviceRunnable;

import org.apache.hadoop.fs.Path;

import java.util.Stack;

/**
 * Created by zhangxiaofan on 2018/11/27.
 */
public class StackElement {

    private Stack<Path> stack = new Stack<Path>();

    public StackElement() {
    }
    public Stack getStack() {
        return stack;
    }
    public void push(Path path){
        stack.push(path);
    }
    public Path pop(){
        Path hdfspath = null;
        synchronized (StackElement.class){
            hdfspath = stack.pop();
        }
        return hdfspath;
    }
    public boolean empty(){
        boolean falg = false;
        synchronized (StackElement.class){
            falg = stack.empty();
        }
        return falg;
    }
}
