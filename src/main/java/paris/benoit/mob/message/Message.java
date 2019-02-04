package paris.benoit.mob.message;

// String for now. Genericity incoming? JSON? Tuples? Types?
public class Message {
    
    String content = "";

    public Message(String content) {
        super();
        this.content = content;
    }

    public String getContent() {
        return content;
    }

    @Override
    public String toString() {
        return "Message [content=" + content + "]";
    }
    
}
