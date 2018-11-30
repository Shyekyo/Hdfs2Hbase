package com.nokia.share.DimModel;

import java.io.Serializable;

public class SgwInfo implements Serializable{
	private static final long serialVersionUID = 9041890258632078369L;
	String ip;
	String name;
	String oid;

	public SgwInfo(String ip, String name, String oid) {
		this.ip = ip;
		this.name = name;
		this.oid = oid;
	}

	public SgwInfo() {
	}

	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getOid() {
		return oid;
	}
	public void setOid(String oid) {
		this.oid = oid;
	}
	@Override
	public String toString() {
		return "SgwInfo [ip=" + ip + ", name=" + name + ", oid=" + oid + "]";
	}
	
}
