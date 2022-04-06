package com.link_intersystems.dbunit.dataset.dbunit.dataset.bean;

import java.util.Date;
import java.util.Objects;

import static com.link_intersystems.dbunit.dataset.util.DataFormatter.YYYY_MM_DD;

public class EmployeeBean {


    public static EmployeeBean king() {
        EmployeeBean king = new EmployeeBean(7839, "KING");
        king.setJob("PRESIDENT");
        king.setSal(7698);
        king.setHireDate(YYYY_MM_DD.safeParse("1981-11-17"));
        king.setDepartmentNumber(10);
        return king;
    }

    public static EmployeeBean blake() {
        EmployeeBean blake = new EmployeeBean(7839, "BLAKE");
        blake.setJob("MANAGER");
        blake.setSal(7839);
        blake.setHireDate(YYYY_MM_DD.safeParse("1-5-1981"));
        blake.setDepartmentNumber(20);
        return blake;
    }

    public static EmployeeBean clark() {
        EmployeeBean clark = new EmployeeBean(7782, "CLARK");
        clark.setJob("MANAGER");
        clark.setSal(7839);
        clark.setHireDate(YYYY_MM_DD.safeParse("9-6-1981"));
        clark.setDepartmentNumber(30);
        return clark;
    }

    public static EmployeeBean jones() {
        EmployeeBean jones = new EmployeeBean(7566, "JONES");
        jones.setJob("MANAGER");
        jones.setSal(7839);
        jones.setHireDate(YYYY_MM_DD.safeParse("2-4-1981"));
        jones.setDepartmentNumber(20);
        return jones;
    }


    private int number;
    private String name;
    private String job;
    private Date hireDate;
    private int sal;
    private int departmentNumber;

    public EmployeeBean(int number, String name) {
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

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public Date getHireDate() {
        return hireDate;
    }

    public void setHireDate(Date hireDate) {
        this.hireDate = hireDate;
    }

    public int getSal() {
        return sal;
    }

    public void setSal(int sal) {
        this.sal = sal;
    }

    public void setDepartmentNumber(int departmentNumber) {
        this.departmentNumber = departmentNumber;
    }

    public int getDepartmentNumber() {
        return departmentNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EmployeeBean that = (EmployeeBean) o;
        return number == that.number && sal == that.sal && departmentNumber == that.departmentNumber && name.equals(that.name) && job.equals(that.job) && hireDate.equals(that.hireDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(number, name, job, hireDate, sal, departmentNumber);
    }
}