package org.imsi.queryEREngine.imsi.er;

public class KDebug {
	public static String getCallerClassName() {
		StackTraceElement[] stElements = Thread.currentThread().getStackTrace();
		for (int i=1; i<stElements.length; i++) {
			StackTraceElement ste = stElements[i];
			if (!ste.getClassName().equals(KDebug.class.getName()) && ste.getClassName().indexOf("java.lang.Thread")!=0) {
				return ste.getClassName();
			}
		}
		return null;
	}


}