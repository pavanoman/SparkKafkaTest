public class User {


    private int id;
    private String name;
    private long time;

    public User(int id, String name, long time) {
        this.id = id;
        this.name = name;
        this.time = time;
    }



    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public long getTime() {
        return time;
    }


}
