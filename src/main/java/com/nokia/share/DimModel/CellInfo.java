package com.nokia.share.DimModel;

import java.io.Serializable;

public class CellInfo implements Serializable{
	private static final long serialVersionUID = -6416600151653848471L;
	String cellId;
	String eci;
	String cellName;
	String cellOid;
	String subRegionOid;
	String areaId;
	String mscOid;
	String cell_sac;
	String lac;
	String town_oid;
	int neType;
	
	public String getCellId() {
		return cellId;
	}
	public void setCellId(String cellId) {
		this.cellId = cellId;
	}
	public String getEci() {
		return eci;
	}
	public void setEci(String eci) {
		this.eci = eci;
	}
	public String getCellName() {
		return cellName;
	}
	public void setCellName(String cellName) {
		this.cellName = cellName;
	}
	public String getCellOid() {
		return cellOid;
	}
	public void setCellOid(String cellOid) {
		this.cellOid = cellOid;
	}
	public String getSubRegionOid() {
		return subRegionOid;
	}
	public void setSubRegionOid(String subRegionOid) {
		this.subRegionOid = subRegionOid;
	}
	public String getAreaId() {
		return areaId;
	}
	public void setAreaId(String areaId) {
		this.areaId = areaId;
	}
	public String getMscOid() {
		return mscOid;
	}
	public void setMscOid(String mscOid) {
		this.mscOid = mscOid;
	}
	public int getNeType() {
		return neType;
	}
	public void setNeType(int neType) {
		this.neType = neType;
	}
	public String getCell_sac() {
		return cell_sac;
	}
	public void setCell_sac(String cell_sac) {
		this.cell_sac = cell_sac;
	}
	public String getLac() {
		return lac;
	}
	public void setLac(String lac) {
		this.lac = lac;
	}
	public String getTown_oid() {
		return town_oid;
	}
	public void setTown_oid(String town_oid) {
		this.town_oid = town_oid;
	}
	@Override
	public String toString() {
		return "CellInfo [cellId=" + cellId + ", eci=" + eci + ", cellName=" + cellName + ", cellOid=" + cellOid
				+ ", subRegionOid=" + subRegionOid + ", areaId=" + areaId + ", mscOid=" + mscOid + ", cell_sac="
				+ cell_sac + ", lac=" + lac + ", town_oid=" + town_oid + ", neType=" + neType + "]";
	}
	
}
