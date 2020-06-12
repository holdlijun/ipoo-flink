package common;

public class Item {
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String name;
    public Integer id;

    public Item() {
    }


    @Override
    public String toString() {
        return "Item{" +
                "name='" + name + '\'' +
                ", id=" + id +
                '}';
    }
}
