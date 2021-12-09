import entity.UserEntity;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

public class test {
    @Test
    public void fun1() {
        Configuration conf = new Configuration().configure();
        SessionFactory sessionFactory = conf.buildSessionFactory();
        Session session = sessionFactory.openSession();
        Transaction transaction = session.beginTransaction();
        UserEntity user = new UserEntity();
        user.setId(1);
        user.setName("zhangsan");
        session.save(user);
        UserEntity user2 = new UserEntity();
        user2.setId(2);
        user2.setName("lisi");
        session.save(user2);
        transaction.commit();
        System.out.println(session.createQuery("from UserEntity").list());
        session.close();
        sessionFactory.close();
        System.out.println("end");
    }
}



