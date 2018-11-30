package com.nokia.share.DimModel;

import java.io.Serializable;

public class CompanyIPInfo implements Serializable{
	private static final long serialVersionUID = 1749677270847286457L;
	String appServerIp;
	String appType;
	String appSubType;
	String company;
	
	public String getAppServerIp() {
		return appServerIp;
	}
	public void setAppServerIp(String appServerIp) {
		this.appServerIp = appServerIp;
	}
	public String getAppType() {
		return appType;
	}
	public void setAppType(String appType) {
		this.appType = appType;
	}
	public String getAppSubType() {
		return appSubType;
	}
	public void setAppSubType(String appSubType) {
		this.appSubType = appSubType;
	}
	public String getCompany() {
		return company;
	}
	public void setCompany(String company) {
		this.company = company;
	}
	
	@Override
	public String toString() {
		return "CompanyIPInfo [appServerIp=" + appServerIp + ", appType=" + appType + ", appSubType=" + appSubType
				+ ", company=" + company + "]";
	}
	
}
