package rmi;

import java.io.Serializable;
import java.lang.reflect.Method;

public class MethodCall implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = -4724209998802410954L;
    String name;
    Class[] types;
    Object[] params;

    public MethodCall(Method method, Object[] params) {
        name = method.getName();
        this.params = params;
        types = method.getParameterTypes();
    }

}
