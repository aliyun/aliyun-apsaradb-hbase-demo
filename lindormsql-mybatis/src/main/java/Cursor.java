public class Cursor {
  private int id;
  private String record;

  private String next;

  public Cursor(int id, String record)
  {
    this.id = id;
    this.record = record;
    this.next = null;
  }

  public Cursor(int id, String record, String next)
  {
    this.id = id;
    this.record = record;
    this.next = next;
  }

  public int getId()
  {
    return this.id;
  }

  public void setId(int id)
  {
    this.id = id;
  }

  public String getRecord()
  {
    return this.record;
  }

  public void setRecord(String record)
  {
    this.record = record;
  }

  public String getNext()
  {
    return this.next;
  }

  public void setNext(String next)
  {
    this.next = next;
  }

  @Override
  public String toString()
  {
    return next == null ?
        "Cursor(id=" + id + ",record=" + record + ")" :
        "Cursor(id=" + id + ",record=" + record + ",_l_cursor_=" + next + ")";
  }
}
