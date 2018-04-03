package com.analysis.findata.FinData;

public class FinDataTwo {
	private String date;
	private String smsReq;
	private String bankName;
	private String mobileNo;
	private String dndStatus;
	private String actNumber;
	private String amt;
	private String msgDate;
	private String shareName;
	private String numberShare;
	private String avgShare;
	private int template;

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getSmsReq() {
		return smsReq;
	}

	public void setSmsReq(String smsReq) {
		if (smsReq.equals("SMS_REQ")) {
			this.smsReq = "YES";
		} else {
			this.smsReq = "NO";
		}

	}

	public String getBankName() {
		return bankName;
	}

	public void setBankName(String bankName) {
		this.bankName = bankName;
	}

	public String getMobileNo() {
		return mobileNo;
	}

	public void setMobileNo(String mobileNo) {
		this.mobileNo = mobileNo;
	}

	public String getDndStatus() {
		return dndStatus;
	}

	public void setDndStatus(String dndStatus) {
		if (dndStatus.equals("DNDF-0")) {
			this.dndStatus = "ON";
		} else {
			this.dndStatus = "OFF";
		}

	}

	public String getActNumber() {
		return actNumber;
	}

	public void setActNumber(String actNumber) {
		this.actNumber = actNumber;
	}

	public String getAmt() {
		return amt;
	}

	public void setAmt(String amt) {
		this.amt = amt;
	}

	public String getMsgDate() {
		return msgDate;
	}

	public void setMsgDate(String msgDate) {
		this.msgDate = msgDate;
	}

	public String getShareName() {
		return shareName;
	}

	public void setShareName(String shareName) {
		this.shareName = shareName;
	}

	public String getNumberShare() {
		return numberShare;
	}

	public void setNumberShare(String numberShare) {
		this.numberShare = numberShare;
	}

	public String getAvgShare() {
		return avgShare;
	}

	public void setAvgShare(String avgShare) {
		this.avgShare = avgShare;
	}

	public int getTemplate() {
		return template;
	}

	public void setTemplate(int template) {
		this.template = template;
	}
	
}
