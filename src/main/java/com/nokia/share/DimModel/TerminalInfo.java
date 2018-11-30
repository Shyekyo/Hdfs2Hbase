package com.nokia.share.DimModel;

import java.io.Serializable;

public class TerminalInfo implements Serializable{
	private static final long serialVersionUID = -4287497606829078931L;
	String code;
	String brandName;
	String name;

	public TerminalInfo(String code, String brandName, String name) {
		this.code = code;
		this.brandName = brandName;
		this.name = name;
	}

	public TerminalInfo() {
	}

	public String getCode() {
		return code;
	}
	public void setCode(String code) {
		this.code = code;
	}
	public String getBrandName() {
		return brandName;
	}
	public void setBrandName(String brandName) {
		this.brandName = brandName;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	@Override
	public String toString() {
		return "TerminalInfo [code=" + code + ", brandName=" + brandName + ", name=" + name + "]";
	}
	
}
