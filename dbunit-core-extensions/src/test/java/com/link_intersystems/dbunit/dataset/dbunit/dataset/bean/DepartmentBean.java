package com.link_intersystems.dbunit.dataset.dbunit.dataset.bean;

/**
 *  @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DepartmentBean {

    public static DepartmentBean accounting() {
        DepartmentBean accounting = new DepartmentBean(10, "ACCOUNTING");
        accounting.setLoc("NEW YORK");
        return accounting;
    }

    public static DepartmentBean research() {
        DepartmentBean accounting = new DepartmentBean(20, "RESEARCH");
        accounting.setLoc("DALLAS");
        return accounting;
    }

    public static DepartmentBean sales() {
        DepartmentBean accounting = new DepartmentBean(30, "SALES");
        accounting.setLoc("CHICAGO");
        return accounting;
    }

    public static DepartmentBean operations() {
        DepartmentBean accounting = new DepartmentBean(40, "OPERATIONS");
        accounting.setLoc("BOSTON");
        return accounting;
    }

    private int number;
    private String name;
    private String loc;

    public DepartmentBean(int number, String name) {
        this.number = number;
        this.name = name;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLoc() {
        return loc;
    }

    public void setLoc(String loc) {
        this.loc = loc;
    }
}
