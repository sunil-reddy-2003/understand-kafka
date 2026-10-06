package com.msr.producer.model;

public class Family {
    private String father;
    private int fAge;
    private String mother;
    private int mAge;

    public Family() {
    }

    public Family(String father, int fAge, String mother, int mAge) {
        this.father = father;
        this.fAge = fAge;
        this.mother = mother;
        this.mAge = mAge;
    }

    public String getFather() {
        return father;
    }

    public void setFather(String father) {
        this.father = father;
    }

    public int getfAge() {
        return fAge;
    }

    public void setfAge(int fAge) {
        this.fAge = fAge;
    }

    public String getMother() {
        return mother;
    }

    public void setMother(String mother) {
        this.mother = mother;
    }

    public int getmAge() {
        return mAge;
    }

    public void setmAge(int mAge) {
        this.mAge = mAge;
    }

    @Override
    public String toString() {
        return "Course{" +
                "father='" + father + '\'' +
                ", fAge=" + fAge +
                ", mother='" + mother + '\'' +
                ", mAge=" + mAge +
                '}';
    }
}
