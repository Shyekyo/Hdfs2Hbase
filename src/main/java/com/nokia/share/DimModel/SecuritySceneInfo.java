package com.nokia.share.DimModel;

import java.io.Serializable;

public class SecuritySceneInfo implements Serializable{

	private static final long serialVersionUID = -4134678549408232141L;
	String securityScene;
    String securitySceneLv3;
    String securitySceneLv2;
    String securitySceneLv1;
    String ownName;
    int status;
	public String getSecurityScene() {
		return securityScene;
	}
	public void setSecurityScene(String securityScene) {
		this.securityScene = securityScene;
	}
	public String getSecuritySceneLv3() {
		return securitySceneLv3;
	}
	public void setSecuritySceneLv3(String securitySceneLv3) {
		this.securitySceneLv3 = securitySceneLv3;
	}
	public String getSecuritySceneLv2() {
		return securitySceneLv2;
	}
	public void setSecuritySceneLv2(String securitySceneLv2) {
		this.securitySceneLv2 = securitySceneLv2;
	}
	public String getSecuritySceneLv1() {
		return securitySceneLv1;
	}
	public void setSecuritySceneLv1(String securitySceneLv1) {
		this.securitySceneLv1 = securitySceneLv1;
	}
	public String getOwnName() {
		return ownName;
	}
	public void setOwnName(String ownName) {
		this.ownName = ownName;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	@Override
	public String toString() {
		return "SecuritySceneInfo [securityScene=" + securityScene + ", securitySceneLv3=" + securitySceneLv3
				+ ", securitySceneLv2=" + securitySceneLv2 + ", securitySceneLv1=" + securitySceneLv1 + ", status="
				+ status + "]";
	}
}
