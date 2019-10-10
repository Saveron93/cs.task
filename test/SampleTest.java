import com.credit.Database;
import com.credit.Event;
import org.junit.Assert;
import org.junit.Test;

public class SampleTest {
    @Test
    public void getTimestampIfExistsTest() {
        Database database = new Database("test");
        Event first = new Event();
        first.id = "blah";
        first.state = Event.State.STARTED;
        first.timestamp = 100;
        database.ProcessEvent(first);
        Event second = new Event();
        second.id = "blah";
        second.state = Event.State.FINISHED;
        second.timestamp = 103;
        database.ProcessEvent(second);
        Assert.assertEquals(database.GetResultDuration("blah"), 3);
    }
}
