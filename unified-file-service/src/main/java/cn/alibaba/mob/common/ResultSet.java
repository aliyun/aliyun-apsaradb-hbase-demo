package cn.alibaba.mob.common;

import java.io.Serializable;

public class ResultSet<T> implements Serializable {
	
	private static final long serialVersionUID = 7372208613317463413L;

	private boolean isSuccess;

    private int code;

    private String message;

    private String tips;

    private T result;

    private ResultSet(boolean isSuccess, int code, String message, T result, String tips) {
        this.isSuccess = isSuccess;
        this.code = code;
        this.message = message;
        this.result = result;
        this.tips = tips;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public T getResult() {
        return result;
    }

    public void setResult(T t) {
        this.result = t;
    }

    public String getTips() {
        return tips;
    }

    public void setTips(String tips) {
        this.tips = tips;
    }

    public static <T> ResultSet<T> valueOf(ResultSetCode resultSetCode) {
        return valueOf(resultSetCode, null);
    }

    public static <T> ResultSet<T> valueOf(ResultSetCode resultSetCode, String tips) {
        if (resultSetCode == ResultSetCode.SUCCESS) {
            return new ResultSet<T>(true, resultSetCode.getCode(), resultSetCode.getMessage(), null, tips);
        } else {
            return new ResultSet<T>(false, resultSetCode.getCode(), resultSetCode.getMessage(), null, tips);
        }
    }

    public static <T> ResultSet<T> valueOf(ResultSetCode resultSetCode, T result) {
        if (resultSetCode == ResultSetCode.SUCCESS) {
            return new ResultSet<T>(true, resultSetCode.getCode(), resultSetCode.getMessage(), result, null);
        } else {
            return new ResultSet<T>(false, resultSetCode.getCode(), resultSetCode.getMessage(), result, null);
        }
    }
	
}

