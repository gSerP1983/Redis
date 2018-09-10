using Microsoft.VisualStudio.TestTools.UnitTesting;
using StackExchange.Redis;

namespace Test.Redis
{
    [TestClass]
    public class PubSubTest
    {
        [TestInitialize]
        public void TestInitialize()
        {       
        }

        [TestMethod]
        public void TestPing()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var sub = redis.GetSubscriber();
            Assert.IsNotNull(sub.Ping());
        }

        [TestCleanup]
        public void TestCleanup()
        {
        }
    }
}
