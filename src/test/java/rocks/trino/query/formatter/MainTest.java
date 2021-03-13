package rocks.trino.query.formatter;

import org.junit.Assert;
import org.junit.Test;

public class MainTest
{
    @Test
    public void test()
    {
        Main.Response response = Main.parse(
                new Main.Request("select * from e", false));

        Assert.assertEquals(1, response.suggestions.size());
        Assert.assertTrue(response.suggestions.contains("events"));
    }

    @Test
    public void test2()
    {
        Main.Response response = Main.parse(
                new Main.Request("select * from events where events.t", false));

        Assert.assertTrue(response.suggestions.contains("type"));
    }
}
