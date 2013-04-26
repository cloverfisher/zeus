package com.taobao.zeus.store;

import java.util.Arrays;
import java.util.List;

public class Super {

	private static final List<String> supers = Arrays.asList(
			"TAOBAO-HZ\\zhoufang", "TAOBAO-HZ\\gufei.wzy");

	public static List<String> getSupers() {
		return supers;
	}
}
